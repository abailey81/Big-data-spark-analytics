import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Step 1: Load the results CSV from S3 or local disk
csv_file_path = "september_2015_document.csv"  # Ensure you download the file locally or adjust the path
result_df = pd.read_csv(csv_file_path)

# Step 2: Generate a timestamp for filenames
now = datetime.now()
timestamp = now.strftime("%d-%m-%Y_%H-%M-%S")

# Step 3: Create Histogram for Block Count
plt.figure(figsize=(10, 6))
plt.bar(result_df['formatted_date'], result_df['block_count'], color='blue')
plt.xlabel('Formatted Date')
plt.ylabel('Block Count')
plt.title('Block Count per Day - September 2015')
plt.xticks(rotation=45, fontsize=10)
plt.tight_layout()
block_count_histogram_path = f"block_count_histogram_{timestamp}.png"
plt.savefig(block_count_histogram_path)
print(f"Block count histogram saved as: {block_count_histogram_path}")
plt.close()

# Step 4: Create Histogram for Unique Senders Count
plt.figure(figsize=(10, 6))
plt.bar(result_df['formatted_date'], result_df['unique_senders_count_number'], color='green')
plt.xlabel('Formatted Date')
plt.ylabel('Unique Senders Count Number')
plt.title('Unique Senders Count per Day - September 2015')
plt.xticks(rotation=45, fontsize=10)
plt.tight_layout()
unique_senders_histogram_path = f"unique_senders_histogram_{timestamp}.png"
plt.savefig(unique_senders_histogram_path)
print(f"Unique senders count histogram saved as: {unique_senders_histogram_path}")
plt.close()