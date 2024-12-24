from tqdm import tqdm
import time

# Example usage of tqdm with a for loop
for i in tqdm(range(100)):
    time.sleep(0.2)  # Simulate some work being done

print("Task completed!")
