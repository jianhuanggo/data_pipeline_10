import os
from openai import OpenAI
class APIOpenAI:
    def __init__(self):
        self.client = OpenAI(
            base_url="http://localhost:1234/v1"
            # api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
        )

    def chat(self, message: str):
        return self.client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": message,
                }
            ],
            model="gpt-4o",
        )
