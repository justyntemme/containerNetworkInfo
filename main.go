package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	rateLimit       = 30
	rateLimitPeriod = 30 * time.Second
	workerThreads   = 6
)

var (
	tlURL        string
	token        string
	requestChan  = make(chan map[string]interface{})
	outputChan   = make(chan map[string]interface{})
	wg           = sync.WaitGroup{}
	consumerWg   = sync.WaitGroup{}
	debug        bool
)

func init() {
	flag.BoolVar(&debug, "debug", false, "show debug messages")
	flag.Parse()

	tlURL = os.Getenv("tlUrl")
	if tlURL == "" {
		log.Fatal("Missing tlUrl environment variable")
	}

	if debug {
		log.Println("Debug logging is enabled")
	}
}

func main() {
	pcIdentity := mustGetEnv("pcIdentity")
	pcSecret := mustGetEnv("pcSecret")

	statusCode, cwpToken := generateCwpToken(pcIdentity, pcSecret)
	if statusCode != http.StatusOK || cwpToken == "" {
		log.Fatal("Failed to generate token")
	}
	if debug {
		log.Println("Successfully generated authentication token")
	}
	token = cwpToken

	// Start producer thread
	wg.Add(1)
	go producer(cwpToken, 100)

	// Start consumer threads
	for i := 0; i < workerThreads; i++ {
		consumerWg.Add(1)
		go consumer()
	}

	// Start output thread - close output channel after all consumers finish
	go func() {
		consumerWg.Wait()
		close(outputChan)
	}()

	// Start outputter
	wg.Add(1)
	go outputter()

	// Wait for producer and outputter to finish
	wg.Wait()
	
	if debug {
		log.Println("Program completed successfully")
	}
}

func producer(token string, limit int) {
	defer wg.Done()

	offset := 0
	requestCount := 0
	startTime := time.Now()

	for {
		if requestCount >= rateLimit {
			elapsed := time.Since(startTime)
			if elapsed < rateLimitPeriod {
				sleepTime := rateLimitPeriod - elapsed
				log.Printf("Rate limit reached. Sleeping for %v seconds...\n", sleepTime.Seconds())
				time.Sleep(sleepTime)
			}
			requestCount = 0
			startTime = time.Now()
		}

		statusCode, response := getContainers(token, offset, limit)
		requestCount++

		if statusCode != http.StatusOK {
			log.Printf("Error fetching containers: %d\n", statusCode)
			if debug {
				log.Printf("Response body: %s\n", string(response))
			}
			break
		}

		var containers []map[string]interface{}
		if err := json.Unmarshal(response, &containers); err != nil {
			log.Printf("Error parsing response: %v\n", err)
			if debug && len(response) > 0 {
				responsePreview := string(response)
				if len(responsePreview) > 500 {
					responsePreview = responsePreview[:500]
				}
				log.Printf("Response body (first 500 chars): %s\n", responsePreview)
			}
			break
		}

		if len(containers) == 0 {
			if debug {
				log.Println("No more containers to process")
			}
			break
		}

		if debug {
			log.Printf("Fetched %d containers (offset: %d)\n", len(containers), offset)
		}

		for _, container := range containers {
			requestChan <- container
		}

		if len(containers) < limit {
			break
		}

		offset += limit
	}

	close(requestChan)
}

func consumer() {
	defer consumerWg.Done()

	processedCount := 0
	for container := range requestChan {
		processedCount++
		containerInfo := extractNetworkInfo(container)
		if containerInfo != nil && len(containerInfo) > 0 {
			if debug {
				log.Printf("Consumer found container with open ports: %v\n", containerInfo["id"])
			}
			outputChan <- containerInfo
		} else if debug {
			log.Printf("Consumer: container %v has no open ports\n", container["_id"])
		}
	}
	if debug {
		log.Printf("Consumer finished, processed %d containers\n", processedCount)
	}
}

func outputter() {
	defer wg.Done()

	outputCount := 0
	for containerInfo := range outputChan {
		outputCount++
		output, err := json.MarshalIndent(containerInfo, "", "  ")
		if err != nil {
			log.Printf("Error encoding JSON: %v\n", err)
			continue
		}
		fmt.Println(string(output))
	}
	if debug {
		log.Printf("Outputter finished, output %d containers\n", outputCount)
	}
}
//TODO add support for hosts alongside container support
func getContainers(token string, offset, limit int) (int, []byte) {
	// Don't use fields parameter - get all container data including network info and image
	containersURL := fmt.Sprintf("%s/api/v1/containers?offset=%d&limit=%d", tlURL, offset, limit)

	req, err := http.NewRequest("GET", containersURL, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v\n", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("HTTP request failed: %v\n", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v\n", err)
	}

	return resp.StatusCode, body
}

func extractNetworkInfo(container map[string]interface{}) map[string]interface{} {
	openPorts := []map[string]interface{}{}

	if debug {
		// Log available keys in container to see what fields are present
		keys := make([]string, 0, len(container))
		for k := range container {
			keys = append(keys, k)
		}
		log.Printf("Container ID: %v, Available fields: %v\n", container["_id"], keys)
		// Log image field if present
		if imageVal, ok := container["image"]; ok {
			log.Printf("Container %v has image field: %v (type: %T)\n", container["_id"], imageVal, imageVal)
		} else {
			log.Printf("Container %v does NOT have image field\n", container["_id"])
		}
	}

	// Extract from 'network' object
	if network, ok := container["network"].(map[string]interface{}); ok {
		if debug {
			log.Printf("Found network object for container %v\n", container["_id"])
		}
		if ports, ok := network["ports"].([]interface{}); ok {
			for _, portObj := range ports {
				if port, ok := portObj.(map[string]interface{}); ok {
					openPorts = append(openPorts, map[string]interface{}{
						"port":     port["container"],
						"host_port": port["host"],
						"host_ip":   port["hostIP"],
						"nat":       port["nat"],
						"type":      "network",
					})
				}
			}
		}
	}

	// Extract from 'networkSettings' object
	if networkSettings, ok := container["networkSettings"].(map[string]interface{}); ok {
		if debug {
			log.Printf("Found networkSettings object for container %v\n", container["_id"])
		}
		if ports, ok := networkSettings["ports"].([]interface{}); ok {
			for _, portObj := range ports {
				if port, ok := portObj.(map[string]interface{}); ok {
					openPorts = append(openPorts, map[string]interface{}{
						"port":     port["containerPort"],
						"host_port": port["hostPort"],
						"host_ip":   port["hostIP"],
						"type":      "networkSettings",
					})
				}
			}
		}
	}

	// Extract from 'firewallProtection' object
	if firewallProtection, ok := container["firewallProtection"].(map[string]interface{}); ok {
		if debug {
			log.Printf("Found firewallProtection object for container %v\n", container["_id"])
			// Log what's in firewallProtection
			fwKeys := make([]string, 0, len(firewallProtection))
			for k := range firewallProtection {
				fwKeys = append(fwKeys, k)
			}
			log.Printf("firewallProtection fields: %v\n", fwKeys)
		}
		if ports, ok := firewallProtection["ports"].([]interface{}); ok {
			for _, port := range ports {
				openPorts = append(openPorts, map[string]interface{}{
					"port": port,
					"type": "firewallProtection",
				})
			}
		}

		if tlsPorts, ok := firewallProtection["tlsPorts"].([]interface{}); ok {
			for _, port := range tlsPorts {
				openPorts = append(openPorts, map[string]interface{}{
					"port": port,
					"type": "firewallProtection_tls",
				})
			}
		}

		if unprotectedProcesses, ok := firewallProtection["unprotectedProcesses"].([]interface{}); ok {
			for _, processObj := range unprotectedProcesses {
				if process, ok := processObj.(map[string]interface{}); ok {
					openPorts = append(openPorts, map[string]interface{}{
						"port":    process["port"],
						"process": process["process"],
						"tls":     process["tls"],
						"type":    "unprotectedProcess",
					})
				}
			}
		}
	}

	if len(openPorts) > 0 {
		result := map[string]interface{}{
			"id":         container["_id"],
			"open_ports": openPorts,
		}
		
		// Extract image name if available - try multiple field names and structures
		imageName := extractImageName(container)
		if imageName != "" {
			result["image"] = imageName
		}
		
		return result
	}

	return nil
}

// extractImageName tries multiple ways to extract the image name from container data
func extractImageName(container map[string]interface{}) string {
	// Try direct "image" field as string
	if image, ok := container["image"].(string); ok && image != "" {
		return image
	}
	
	// Try "image" as object with various nested fields
	if imageObj, ok := container["image"].(map[string]interface{}); ok {
		// Try "name" field
		if imageName, ok := imageObj["name"].(string); ok && imageName != "" {
			return imageName
		}
		// Try "id" field
		if imageID, ok := imageObj["id"].(string); ok && imageID != "" {
			return imageID
		}
		// Try "repo" + "tag" combination
		if repo, ok := imageObj["repo"].(string); ok && repo != "" {
			if tag, ok := imageObj["tag"].(string); ok && tag != "" {
				return repo + ":" + tag
			}
			return repo
		}
		// Try "imageName" field
		if imageName, ok := imageObj["imageName"].(string); ok && imageName != "" {
			return imageName
		}
	}
	
	// Try alternative field names
	if imageName, ok := container["imageName"].(string); ok && imageName != "" {
		return imageName
	}
	if imageID, ok := container["imageId"].(string); ok && imageID != "" {
		return imageID
	}
	if imageRepo, ok := container["imageRepo"].(string); ok && imageRepo != "" {
		if imageTag, ok := container["imageTag"].(string); ok && imageTag != "" {
			return imageRepo + ":" + imageTag
		}
		return imageRepo
	}
	
	// Try nested in "info" or other common objects
	if info, ok := container["info"].(map[string]interface{}); ok {
		if image, ok := info["image"].(string); ok && image != "" {
			return image
		}
		if imageName, ok := info["imageName"].(string); ok && imageName != "" {
			return imageName
		}
	}
	
	return ""
}

func generateCwpToken(accessKey, accessSecret string) (int, string) {
	if tlURL == "" {
		log.Fatalf("Missing TL_URL environment variable")
	}

	authURL := fmt.Sprintf("%s/api/v1/authenticate", tlURL)

	requestBody := map[string]string{
		"username": accessKey,
		"password": accessSecret,
	}
	requestBytes, _ := json.Marshal(requestBody)

	req, err := http.NewRequest("POST", authURL, bytes.NewBuffer(requestBytes))
	if err != nil {
		log.Fatalf("Error creating request: %v\n", err)
	}

	req.Header.Set("accept", "application/json; charset=UTF-8")
	req.Header.Set("content-type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("HTTP request failed: %v\n", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v\n", err)
	}

	if resp.StatusCode == http.StatusOK {
		var responseData map[string]interface{}
		if err := json.Unmarshal(body, &responseData); err != nil {
			log.Fatalf("Error parsing response: %v\n", err)
		}
		return http.StatusOK, responseData["token"].(string)
	}

	log.Printf("Unable to acquire token: %d\n", resp.StatusCode)
	return resp.StatusCode, ""
}

func mustGetEnv(key string) string {
	value, found := os.LookupEnv(key)
	if !found {
		log.Fatalf("Missing %s environment variable", key)
	}
	return value
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

