from mftool import Mftool
mf = Mftool()

# Get a list of all schemes
all_scheme_codes = mf.get_scheme_codes()

# Find the scheme code for the fund you are interested in (e.g., 'DSP Short Term Fund - Regular Plan - Growth')
fund_scheme_code = None
for code, name in all_scheme_codes.items():
    if 'DSP Short Term Fund - Regular Plan - Growth' in name:
        fund_scheme_code = code
        break

if fund_scheme_code:
    # Get the NAV track record, which includes benchmark information
    fund_data = mf.get_nav_track_record(fund_scheme_code)
    print(fund_data)
else:
    print("Fund not found.")

