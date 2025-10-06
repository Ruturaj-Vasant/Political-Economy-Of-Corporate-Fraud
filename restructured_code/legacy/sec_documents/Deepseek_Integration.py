import subprocess
import pandas as pd
import os
import json

# -------------------------------
# Function: build prompt
# -------------------------------
def build_prompt_from_csv(csv_path: str, task_description: str = "Convert this table into JSON format"):
    """
    Reads a CSV file and constructs a text prompt for DeepSeek.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    table_text = df.to_string(index=False)

    prompt = f"""
You are a data assistant.
Task: {task_description}

Here is the table extracted from an SEC filing:
{table_text}

Please provide a structured JSON response.
"""
    return prompt


def build_prompt_from_df(df: pd.DataFrame, task_description: str = "Convert this table into JSON format"):
    """
    Converts a pandas DataFrame directly into a DeepSeek prompt.
    """
    table_text = df.to_string(index=False)

    prompt = f"""
You are a data assistant.
Task: {task_description}

Here is the table extracted from an SEC filing:
{table_text}

Please provide a structured JSON response.
"""
    return prompt


# -------------------------------
# Function: call DeepSeek (via Ollama)
# -------------------------------
def call_deepseek(prompt: str, model: str = "deepseek-r1:14b"):
    """
    Calls the DeepSeek model locally via Ollama with the given prompt.
    """
    try:
        result = subprocess.run(
            ["ollama", "run", model],
            input=prompt.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return result.stdout.decode("utf-8").strip()
    except subprocess.CalledProcessError as e:
        print("Error running DeepSeek:", e.stderr.decode("utf-8"))
        return None


# -------------------------------
# Function: process and save JSON
# -------------------------------
def process_csv_with_deepseek(csv_path: str, task_description: str = "Convert this table into JSON format"):
    """
    Builds a prompt from a CSV, calls DeepSeek, and saves the JSON response.
    """
    prompt = build_prompt_from_csv(csv_path, task_description)
    response = call_deepseek(prompt)

    if response:
        print("\nDeepSeek Response:\n", response)

        # Save response as JSON
        json_path = os.path.splitext(csv_path)[0] + "_deepseek.json"
        try:
            # Attempt to parse model output as JSON
            parsed_json = json.loads(response)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(parsed_json, f, indent=2, ensure_ascii=False)
            print(f"✅ JSON saved to {json_path}")
        except json.JSONDecodeError:
            # If parsing fails, save raw response in a .txt file
            fallback_path = os.path.splitext(csv_path)[0] + "_deepseek.txt"
            with open(fallback_path, "w", encoding="utf-8") as f:
                f.write(response)
            print(f"⚠️ Response not valid JSON, saved raw output to {fallback_path}")
    else:
        print("❌ No response from DeepSeek.")


# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    csv_file = "/path/to/your/1999_cleaned.csv"
    process_csv_with_deepseek(csv_file, "Convert this SEC Summary Compensation Table into structured JSON")