import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from layer3 import DOUBAO_MODELS, generate_text

from sql_tasks import SQL_TASKS


def main() -> None:
    for model in DOUBAO_MODELS:
        print("=" * 80)
        print(f"MODEL: {model}")
        print("=" * 80)
        for task in SQL_TASKS:
            print("-" * 80)
            print(f"TASK: {task['name']}")
            print("-" * 80)
            out = generate_text(task["prompt"], model)
            print(out.strip())
            print()


if __name__ == "__main__":
    main()
