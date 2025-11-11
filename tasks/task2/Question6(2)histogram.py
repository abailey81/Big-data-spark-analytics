import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Step 1: Load the results CSV from the previous computation
csv_file_path = "october_2015_document.csv"  # Adjust the path if needed
result_df = pd.read_csv(csv_file_path)

# Step 2: Generate a timestamp for filenames
now = datetime.now()
timestamp = now.strftime("%d-%m-%Y_%H-%M-%S")

# Step 3: Create a histogram for Total Transaction Fee
plt.figure(figsize=(14, 7))  # Adjust the figure size to match the desired layout
plt.bar(result_df['formatted_date'], result_df['total_transaction_fee'], color='green', label='Total Transaction Fee')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Total Transaction Fee', fontsize=12)
plt.title('Total Transaction Fee per Day in October 2015', fontsize=16)
plt.xticks(rotation=45, fontsize=10)
plt.legend(loc='upper right')  # Adjust legend placement
plt.tight_layout()

# Save the graph
output_path = f"total_transaction_fee_histogram_{timestamp}.png"
plt.savefig(output_path)
print(f"Histogram saved as: {output_path}")
plt.show()