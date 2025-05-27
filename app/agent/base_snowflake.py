from typing import Any, Dict, Optional
import snowflake.connector
from .base import BaseAgent
from ..llm import LLM
from ..config import Config
from ..logger import logger
import re


class BaseSnowflakeAgent(BaseAgent):
    """Base agent for interacting with Snowflake database using LLM for text-to-SQL conversion."""

    def __init__(self, config: Config, name: str, description: str, schema_prompt: str):
        super().__init__(
            name=name,
            description=description,
        )
        logger.info(f"Initializing {name}")
        self.llm = LLM(config_name="snowflake")
        self.conn = None
        self.config = config
        self.schema_prompt = schema_prompt

    async def connect(self, connection_params: Optional[Dict[str, Any]] = None) -> None:
        """Establish connection to Snowflake database."""
        if not connection_params and not self.config.snowflake_config:
            logger.error("No Snowflake configuration found in config.toml")
            raise Exception(
                "No Snowflake connection parameters provided. Please configure Snowflake in config.toml"
            )

        # Get the appropriate configuration based on agent type
        agent_type = self.name.split("_")[
            0
        ]  # Extract 'org' or 'person' from agent name
        snowflake_config = self.config.snowflake_config.get(
            agent_type
        ) or self.config.snowflake_config.get("default")

        if not snowflake_config:
            raise Exception(f"No Snowflake configuration found for {agent_type} agent")

        params = connection_params or {
            "user": snowflake_config.user,
            "password": snowflake_config.password,
            "account": snowflake_config.account,
            "warehouse": snowflake_config.warehouse,
            "database": snowflake_config.database,
            "schema": snowflake_config.schema,
        }

        try:
            logger.info(f"Connecting to Snowflake account: {params['account']}")
            self.conn = snowflake.connector.connect(**params)
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    async def text_to_sql(self, text_query: str) -> str:
        """Convert natural language query to SQL using LLM."""
        logger.info(f"Converting natural language to SQL: {text_query}")
        prompt = f"""
{self.schema_prompt}


#Query: {text_query}
        """

        response = await self.llm.ask([{"role": "user", "content": prompt}])

        # Extract SQL query from code blocks
        sql_match = re.search(r"```sql\n(.*?)```", response, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
        else:
            logger.warning("No SQL code block found in response, using raw response")
            sql_query = response.strip()

        logger.info(f"Generated SQL query: {sql_query}")
        return sql_query

    async def execute_query(self, sql_query: str, max_retries: int = 3) -> Any:
        """Execute SQL query and return results."""
        if not self.conn:
            logger.warning("Not connected to Snowflake, attempting to connect")
            await self.connect()

        # Check for DML/DDL commands
        ddl_dml_commands = [
            "CREATE ",
            "ALTER ",
            "DROP ",
            "TRUNCATE ",
            "INSERT ",
            "DELETE ",
            "UPDATE ",
        ]
        if any(command in sql_query.upper() for command in ddl_dml_commands):
            error_msg = "SQL statement is a DDL/DML which may modify database schema, so we will not be executing the statement at this time"
            logger.error(error_msg)
            raise Exception(error_msg)

        current_retry = 0
        while current_retry < max_retries:
            try:
                logger.info(
                    f"Executing SQL query (attempt {current_retry + 1}/{max_retries}): {sql_query}"
                )
                cursor = self.conn.cursor()
                cursor.execute(sql_query)
                results = cursor.fetchall()
                columns = (
                    [col[0] for col in cursor.description] if cursor.description else []
                )
                cursor.close()
                logger.info(
                    f"Query executed successfully, returned {len(results)} rows"
                )
                return {"columns": columns, "data": results}
            except Exception as e:
                current_retry += 1
                error_msg = f"Failed to execute query (attempt {current_retry}/{max_retries}): {str(e)}"
                logger.error(error_msg)

                if current_retry < max_retries:
                    # Generate new SQL with error context
                    retry_prompt = f"""
                    The previous SQL query failed with error: {str(e)}
                    Original query: {sql_query}
                    Please generate a new SQL query that fixes the error.
                    Remember to:
                    1. Keep the same intent as the original query
                    2. Fix any syntax or semantic errors
                    3. Ensure the query is valid Snowflake SQL
                    4. Wrap the response in ```sql ... ``` blocks
                    """
                    new_sql = await self.text_to_sql(retry_prompt)
                    sql_query = new_sql
                else:
                    raise Exception(
                        f"Failed to execute query after {max_retries} attempts: {str(e)}"
                    )

    async def process_query(self, text_query: str) -> Any:
        """Process natural language query through text-to-SQL conversion and execution."""
        logger.info(f"Processing natural language query: {text_query}")
        sql_query = await self.text_to_sql(text_query)
        results = await self.execute_query(sql_query)

        return results

    async def close(self) -> None:
        """Close the Snowflake connection."""
        if self.conn:
            logger.info("Closing Snowflake connection")
            self.conn.close()
            self.conn = None
            logger.info("Snowflake connection closed")

    async def step(self) -> str:
        """Execute a single step in the agent's workflow."""
        if not self.memory.messages:
            return "No messages to process"

        last_message = self.memory.messages[-1]
        if last_message.role != "user":
            return "Waiting for user query"

        try:
            # Ensure we're connected to Snowflake
            if not self.conn:
                logger.info("No active connection, attempting to connect")
                await self.connect()

            results = await self.process_query(last_message.content)
            self.update_memory("assistant", f"Query results: {results}")
            return f"Successfully processed query: {results}"
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            logger.error(error_msg)
            self.update_memory("assistant", error_msg)
            return error_msg
