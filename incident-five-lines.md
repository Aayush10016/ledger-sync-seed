# Incident Five-Line Note

- **What broke**: A ₹5 water-can transaction was incorrectly recorded as ₹92,213.10.
- **How it was found**: Customers reported incorrect transaction amounts; confirmed via `fixtures/corpus-a.jsonl`.
- **Who/what was affected**: Account 9075 and users with similar transactions where the "available balance" was mistakenly parsed as the transaction amount.
- **Root cause**: The regex parser eagerly captured the last matched currency string in an SMS, grabbing "Avl Bal: Rs.92,213.10" instead of the actual amount "Rs. 5".
- **Why the fix prevents recurrence**: We improved parsing logic to specifically match transaction amounts using directional keywords (debited/credited/spent) and to explicitly ignore strings prefixed with "Bal", "Available Balance", or similar.
