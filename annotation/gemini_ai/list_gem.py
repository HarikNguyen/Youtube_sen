from google import genai
from google.genai import types

# Use your API Key (AI Studio) or set vertexai=True (Google Cloud)
client = genai.Client(api_key="<google_ai_studio_api_here>")

print("Available Models:")
print("-" * 30)
for model in client.models.list():
    print(f"Name: {model.name}")
    print(f"Supported Actions: {model.supported_actions}")
    print("-" * 30)
