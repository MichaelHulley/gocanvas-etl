from datetime import datetime

print("Script started")

value = "06/08/2026 13:22:42"
dt = datetime.strptime(value, "%m/%d/%Y %H:%M:%S")

print(dt)
print("Script finished")