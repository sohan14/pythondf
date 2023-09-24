import pandas as pd
import json
from bs4 import BeautifulSoup
import pdfkit



with open("data.json", "r") as file:
    data_json = json.load(file)

data = data_json['dayRevenueList']
# print(data)
print(len(data_json['dayRevenueList']))
# Column Multi-index setup
col_tuples_grouped = [
    ('', 'Station Name', ''),
    ('', 'Equipment ID', ''),
    ('1.Incoming', 'QR Issuance', 'Count'),
    ('1.Incoming', 'QR Issuance', 'Amount'),
    ('1.Incoming', 'Pass Issue', 'Count'),
    ('1.Incoming', 'Pass Issue', 'Amount'),
    ('1.Incoming', 'QR Adjustment', 'Count'),
    ('1.Incoming', 'QR Adjustment', 'Amount'),
    ('1.Incoming', 'Card Adjustment', 'Count'),
    ('1.Incoming', 'Card Adjustment', 'Amount'),
    ('1.Incoming', 'Paid Exit', 'Count'),
    ('1.Incoming', 'Paid Exit', 'Amount'),
    ('1.Incoming', 'QR Replacement', 'Count'),
    ('1.Incoming', 'QR Replacement', 'Amount'),
    ('1.Incoming', 'Card Exit', 'Count'),
    ('1.Incoming', 'Card Exit', 'Amount'),
    ('2.Outgoing', 'Refund', 'Count'),
    ('2.Outgoing', 'Refund', 'Amount'),
    ('2.Outgoing', 'Cancel', 'Count'),
    ('2.Outgoing', 'Cancel', 'Amount'),
    ('', 'Grand total', 'Total Transaction Count (1+2)'),
    ('', 'Grand total', 'Amount')
]
col_index_grouped = pd.MultiIndex.from_tuples(col_tuples_grouped)

# Data setup
station_data_dict = {}

for entry in data:
    date = entry['recDate']
    station_data_list = []
    for station in entry['stationRevenueList']:
        station_name = station['stationName']
        if station_name == None:
            station_name = "na"
        for device in station['deviceRevenueList']:
            equipment_id = device['equipmentId']
            device_revenue = device['deviceRevenue']
            data_row = [
                station_name,
                equipment_id,
                device_revenue['qrIssue']['count'], device_revenue['qrIssue']['amount'],
                device_revenue['passIssue']['count'], device_revenue['passIssue']['amount'],
                device_revenue['adjustment']['count'], device_revenue['adjustment']['amount'],
                device_revenue['cardAdjustment']['count'], device_revenue['cardAdjustment']['amount'],
                device_revenue['paidExit']['count'], device_revenue['paidExit']['amount'],
                device_revenue['replacement']['count'], device_revenue['replacement']['amount'],
                device_revenue['cardExit']['count'], device_revenue['cardExit']['amount'],
                device_revenue['refund']['count'], device_revenue['refund']['amount'],
                device_revenue['cancel']['count'], device_revenue['cancel']['amount']
            ]
            station_data_list.append(data_row)
    station_data_dict[date] = station_data_list


def find_list_length(data_dict):
    for dat in data_dict.values():
        if dat:  # if the list is not empty
            return len(dat[0])
    return None  # If all lists are empty

list_length = find_list_length(station_data_dict)


# Updating the station_data_dict based on the requirements and the dynamic list length
for date, dat in station_data_dict.items():
    if not dat:  # if the list is empty
        # Append a new list with first two values as 'na' and rest as zeros.
        zero_data = ['na', 'na'] + [0] * (list_length - 2)
        station_data_dict[date].append(zero_data)
# Extracting the mobile issuance data for each date
# print(station_data_dict)
mobile_data = {}

for entry in data:
    try:
        rec_date = entry['recDate']
        count = entry['mobileIssuance']['count']
        amount = entry['mobileIssuance']['amount']
        mobile_data[rec_date] = [count, amount]
    except KeyError:
        # Handle missing keys in the data
        continue

# print(mobile_data)

# Initialize the processed data and index lists
corrected_data_dynamic = []
dynamic_row_index_dynamic = []
date_station_subtotals_dynamic = {}
datewise_subtotals_dynamic = {}

# Get the list of unique dates from both dictionaries
all_dates = set(station_data_dict.keys()) | set(mobile_data.keys())

# Iterate over the unique dates
for date in sorted(all_dates):
    stations = station_data_dict.get(date, [])
    unique_stations = set(row[0] for row in stations)
    datewise_subtotals_dynamic[date] = [0] * (len(stations[0]) - 2) if stations else [0] * 20

    for station_name in unique_stations:
        station_rows = [row for row in stations if row[0] == station_name]

        for row in station_rows:
            if (date, station_name) not in date_station_subtotals_dynamic:
                date_station_subtotals_dynamic[(date, station_name)] = [0] * (len(row) - 2)
            total_Count = sum(row[j] for j in range(2, len(row), 2))
            total_Amount = sum(row[j] for j in range(3, len(row), 2))
            corrected_data_dynamic.append(row + [total_Count, total_Amount])
            for j in range(2, len(row)):
                date_station_subtotals_dynamic[(date, station_name)][j-2] += row[j]
                datewise_subtotals_dynamic[date][j-2] += row[j]
            dynamic_row_index_dynamic.append(date)

        subtotals = date_station_subtotals_dynamic[(date, station_name)]
        subtotal_data = ['Sub Total(' + station_name + ')', '']
        subtotal_total_Count = sum(subtotals[j] for j in range(0, len(subtotals), 2))
        subtotal_total_Amount = sum(subtotals[j] for j in range(1, len(subtotals), 2))
        subtotal_data += subtotals + [subtotal_total_Count, subtotal_total_Amount]
        corrected_data_dynamic.append(subtotal_data)
        dynamic_row_index_dynamic.append(date)

    # Insert mobile data for the given date if present
    if date in mobile_data:
        count, amount = mobile_data[date]
        mobile_data_row = ["Mobile", "", count, amount] + [0] * 18
        corrected_data_dynamic.append(mobile_data_row)
        dynamic_row_index_dynamic.append(date)

        # Adjust datewise subtotals for the mobile data
        datewise_subtotals_dynamic[date][0] += count
        datewise_subtotals_dynamic[date][1] += amount

    # Update datewise subtotal rows
    date_subtotals = datewise_subtotals_dynamic[date]
    date_subtotal_data = ['', '']
    date_subtotal_total_Count = sum(date_subtotals[j] for j in range(0, len(date_subtotals), 2))
    date_subtotal_total_Amount = sum(date_subtotals[j] for j in range(1, len(date_subtotals), 2))
    date_subtotal_data += date_subtotals + [date_subtotal_total_Count, date_subtotal_total_Amount]
    corrected_data_dynamic.append(date_subtotal_data)
    dynamic_row_index_dynamic.append('Sub Total(' + date + ')')

# Calculate the total revenue data
total_revenue_data = [None, None]
for i in range(len(datewise_subtotals_dynamic[next(iter(datewise_subtotals_dynamic))])):
    total_revenue_data.append(sum(datewise_subtotals_dynamic[date][i] for date in all_dates))

# Calculate Grand Total Count and Amount
grand_total_Count = sum(total_revenue_data[j] for j in range(2, len(total_revenue_data), 2))
grand_total_Amount = sum(total_revenue_data[j] for j in range(3, len(total_revenue_data), 2))
total_revenue_data.extend([grand_total_Count, grand_total_Amount])

# Append to the data list and index list
corrected_data_dynamic.append(total_revenue_data)
dynamic_row_index_dynamic.append('Total Revenue')

# Create the DataFrame
dynamic_row_index_dynamic = pd.Index(dynamic_row_index_dynamic, name='Date')
df_corrected_subtotals_dynamic = pd.DataFrame(corrected_data_dynamic, index=dynamic_row_index_dynamic, columns=col_index_grouped)

# Modify the "Total Revenue" row to have empty strings for the first two columns
df_corrected_subtotals_dynamic.loc["Total Revenue", ("", "Station Name", "")] = ""
df_corrected_subtotals_dynamic.loc["Total Revenue", ("", "Equipment ID", "")] = ""

# Calculate Grand Total Amount
incoming_cols = df_corrected_subtotals_dynamic['1.Incoming'].columns.get_level_values(0).unique()
outgoing_cols = df_corrected_subtotals_dynamic['2.Outgoing'].columns.get_level_values(0).unique()
df_corrected_subtotals_dynamic[('', 'Grand total', 'Total Amount(1-2)')] = df_corrected_subtotals_dynamic.apply(
    lambda row: sum(row[('1.Incoming', col, 'Amount')] for col in incoming_cols) - sum(row[('2.Outgoing', col, 'Amount')] for col in outgoing_cols),
    axis=1
)

# Drop the redundant column
df_corrected_subtotals_dynamic = df_corrected_subtotals_dynamic.drop(columns=[('', 'Grand total', 'Amount')])

# Handle NaN values by replacing them with appropriate default values
df_corrected_subtotals_dynamic.fillna("", inplace=True)

# Create a mask to identify repeated values
mask_dynamic = df_corrected_subtotals_dynamic.index.duplicated(keep='first')

# Replace repeated values with empty string
df_corrected_subtotals_dynamic.index = df_corrected_subtotals_dynamic.index.where(~mask_dynamic, '')



# Styling
def alternate_rows_color(s):
    return ['background-color: white' if i % 2 == 0 else 'background-color: #F5F5F5' for i in range(len(s))]

# Reset the index of the DataFrame temporarily for styling
temp_df_for_styling = df_corrected_subtotals_dynamic.reset_index()

# Apply the styling function for alternate rows
styled_temp_df_no_index = temp_df_for_styling.style.apply(alternate_rows_color, axis=0).hide(axis="index")

# Specify the file path where you want to save the Excel file
excel_file_path = 'example.xlsx'

# Use the to_excel() function to save the DataFrame to an Excel file
styled_temp_df_no_index.to_excel(excel_file_path)
# Save the styled DataFrame as HTML without the index
html_output_no_index_corrected = styled_temp_df_no_index.to_html()

# Save the HTML output to a file
output_path_no_index_corrected = "styled_dataframe_no_index_corrected.html"





# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_output_no_index_corrected, "html.parser")

# Find all <th> elements and set their background-color to lightgrey
for header in soup.find_all("th"):
    header["style"] = "background-color: #FAF9F6;"

# Write the modified content to a new HTML file
modified_html_content = str(soup)



# Set borders for all <th> and <td> elements
for cell in soup.find_all(["th", "td"]):
    # If the cell already has a style attribute, append to it. Otherwise, set a new style.
    if "style" in cell.attrs:
        cell["style"] += "border: 1px solid lightgrey;"
    else:
        cell["style"] = "border: 1px solid lightgrey;"

# Additionally, set the border for the table itself
for table in soup.find_all("table"):
    table["style"] = "border-collapse: collapse;"

# Write the modified content to a new HTML file
bordered_html_content = str(soup)
with open(output_path_no_index_corrected, "w") as file:
    file.write(bordered_html_content)



# Path to the HTML file and desired output PDF file
input_html = "styled_dataframe_no_index_corrected.html"
output_pdf = "converted.pdf"

options = {
    'page-size': 'A3',  # Adjust the page size (A4, Letter, etc.)
    'margin-top': '0mm',  # Adjust top margin
    'margin-right': '0mm',  # Adjust right margin
    'margin-bottom': '0mm',  # Adjust bottom margin
    'margin-left': '0mm',  # Adjust left margin
}

# Convert the file to PDF
pdfkit.from_file(input_html, output_pdf, options=options)






