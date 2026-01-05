from openai import OpenAI
import os


class OpenAIClient:
    def __init__(self):
        self.client = OpenAI(api_key=self._load_api_key())

    def _load_api_key(self):
        root = os.getcwd()
        key_path = os.path.join(root, "OPENAI_API_KEY.txt")
        with open(key_path, "r") as f:
            return f.read().strip()

    def complete(self, prompt_or_messages):
        """
        Accepts:
        - string prompt
        - OR list of {role, content} messages
        """

        # ✅ Normalize input
        if isinstance(prompt_or_messages, str):
            messages = [
                {"role": "user", "content": prompt_or_messages}
            ]
        else:
            messages = prompt_or_messages

        response = self.client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=messages,
            temperature=0.3
        )

        return response.choices[0].message.content
