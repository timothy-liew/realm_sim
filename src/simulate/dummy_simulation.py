import json
import os

OUTPUT_PATH = "/opt/airflow/data/dummy_output.json"

def run_simulation():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = []
    else:
        data = []

    # Append new result
    next_run = len(data) + 1
    result = {
        "number_of_days_traveling": next_run,
        "status": "success"
    }
    data.append(result)

    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Run {next_run} complete")
    return result

if __name__ == "__main__":
    run_simulation()
