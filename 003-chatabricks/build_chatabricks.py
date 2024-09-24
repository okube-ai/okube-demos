# Databricks notebook source
# MAGIC %pip install langchain==0.2.14 langchain_community==0.2.12 mlflow==2.15.1 pydantic==2.8.2 cloudpickle==3.0.0 pandas==2.2.2

# COMMAND ----------

from langchain.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks
from langchain.prompts import MessagesPlaceholder

llm = ChatDatabricks(
    endpoint="databricks-dbrx-instruct",
    temperature=0.0,
    top_k=0.95,
)

instructions = """
You are Chatabricks, a helpful, funny and sarcastic assistant helping 
users studying for Databricks certification exams.
"""

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", instructions),
        MessagesPlaceholder("chat_history", optional=True),
        ("user", "{input}"),
        MessagesPlaceholder("agent_scratchpad", optional=True),
    ]
)

chain = prompt | llm

output = chain.invoke({"input": "Who are you?"})
print(output.content)

# COMMAND ----------

import mlflow


class Chatabricks(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        print(f"Predict method received an input of type {type(model_input)}")
        output = chain.invoke({"input": model_input["input"].iloc[0]})
        return {"output": output.content}


mlflow.set_registry_uri("databricks-uc")
model_name = "chatabricks1"
model_full_name = f"sandbox.sample-003.{model_name}"

with mlflow.start_run(run_name=model_name):
    model_info = mlflow.pyfunc.log_model(
        registered_model_name=model_full_name,
        python_model=Chatabricks(),
        pip_requirements=[
            f"mlflow==2.15.1",
            f"langchain==0.2.14",
            f"langchain-community==0.2.12",
        ],
        artifact_path="chatabricks",
        signature=mlflow.models.infer_signature(
            model_input={"input": "Who are you?"},
            model_output={"output": "I am Chatabricks"},
        ),
    )

model = mlflow.pyfunc.load_model(model_info.model_uri)

# Test model
answer = model.predict({"input": "test"})
print("\nAnswer from Chatabricks:")
print(answer["output"])

# COMMAND ----------

from mlflow.types.llm import ChatResponse


class Chatabricks(mlflow.pyfunc.ChatModel):
    def predict(self, context, messages, params=None):
        print(f"Predict method received an input of type {type(messages)}")
        output = chain.invoke({"input": messages[-1].content})
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        response = {
            "id": "0",
            "model": "chatabricks",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": output.content},
                    "finish_reason": "stop",
                }
            ],
            "usage": usage,
        }

        return ChatResponse(**response)


mlflow.set_registry_uri("databricks-uc")
model_name = "chatabricks2"
model_full_name = f"sandbox.sample-003.{model_name}"

with mlflow.start_run(run_name=model_name):
    model_info = mlflow.pyfunc.log_model(
        registered_model_name=model_full_name,
        python_model=Chatabricks(),
        pip_requirements=[
            f"mlflow==2.15.1",
            f"langchain==0.2.14",
            f"langchain-community==0.2.12",
        ],
        artifact_path="chatabricks",
    )

model = mlflow.pyfunc.load_model(model_info.model_uri)

# Test model
answer = model.predict({"messages": [{"role": "user", "content": "Who are you?"}]})
print("\nAnswer from Chatabricks:")
print(answer["choices"][0]["message"]["content"])
