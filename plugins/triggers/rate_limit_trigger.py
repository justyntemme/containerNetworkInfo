# rate_limit_trigger.py

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent


class RateLimitTrigger(BaseTrigger):
    """
    A trigger that defers execution for a specified amount of time.

    This trigger is designed to pause a task to respect an API rate limit
    without holding onto a worker slot.

    :param wait_time: The number of seconds to wait before resuming.
    :param resume_payload: The data to pass back to the operator upon resumption.
    """

    def __init__(self, wait_time: float, resume_payload: Dict[str, Any]):
        super().__init__()
        if wait_time < 0:
            raise ValueError("Wait time must be non-negative.")
        self.wait_time = wait_time
        self.resume_payload = resume_payload

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes the trigger for storage in the database."""
        return (
            # The classpath for this trigger.
            "plugins.triggers.rate_limit_trigger.RateLimitTrigger",
            # The arguments to pass to the __init__ method.
            {"wait_time": self.wait_time, "resume_payload": self.resume_payload},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        The main execution loop for the trigger.

        It simply waits for the specified `wait_time` and then yields a
        TriggerEvent with a 'success' status and the resume_payload.
        """
        self.log.info(f"Trigger deferred for {self.wait_time:.2f} seconds.")
        await asyncio.sleep(self.wait_time)
        self.log.info("Resuming execution.")
        yield TriggerEvent(
            {"status": "success", "resume_payload": self.resume_payload}
        )
