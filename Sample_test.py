from mftool import Mftool

# Initialize mftool instance
mf = Mftool()

# Prompt user for scheme code
scheme_code = input("Enter the mutual fund scheme code: ")

# Fetch latest scheme quote/details
result = mf.get_scheme_quote(scheme_code)

# Print result to terminal (nicely formatted)
if result:
    print("Scheme Details:")
    for key, value in result.items():
        print(f"{key}: {value}")
else:
    print("No data found for the given scheme code.")
