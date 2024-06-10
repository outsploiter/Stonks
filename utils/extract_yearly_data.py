import sys

from bs4 import BeautifulSoup
from datetime import datetime

from utils.stockload import DBUtils

def extract_section_data(soup, section_id, data_tab_id=None):
    current_year = datetime.now().year
    try:
        section = soup.find('section', id=section_id)
    except TypeError as e:
        print("soup is invalid", e)
    if not section:
        raise ValueError(f"Section with id '{section_id}' not found in the HTML content.")

    if data_tab_id:
        tab = section.find('div', id=data_tab_id)
        if not tab:
            raise ValueError(f"Tab with id '{data_tab_id}' not found in section with id '{section_id}'.")
        table = tab.find('table', class_='data-table')
    else:
        table = section.find('table', class_='data-table')

    if not table:
        raise ValueError(f"Table not found in section with id '{section_id}' and tab '{data_tab_id}'.")

    headers = []
    ttm_flag = False
    for header in table.find('thead').find_all('th'):
        header_text = header.get_text(strip=True)
        if header_text == '':
            continue
        header_number = header_text.strip().split(' ')[-1]
        if header_number.isdigit():
            headers.append(int(header_number))
        elif header_text == 'TTM':
            ttm_flag = True
        else:
            print(f"Critical input fount: {header_text}")

    doubt = False
    if (current_year not in headers and current_year-1 not in headers) or (len(headers) < 2):
        doubt = True
    if ttm_flag and headers:
        max_year = max(headers)
        headers.append(max_year+1)

    rows = []
    row_header = []
    for row in table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        row_data = [cell.get_text(strip=True).replace('+', '').replace('%', '').replace(',', '') for cell in cells]
        temp = [float(value) if value != '' else 0 for value in row_data[1:]]
        rows.append(temp)
        if row_data[0] != '':
            curr_header = row_data[0].strip().replace(' ', '_').replace('.', '').strip()
            if curr_header == 'Revenue':
                curr_header = 'Sales'
            elif curr_header == 'Financing_Profit':
                curr_header = 'Operating_Profit'
            elif curr_header == 'Financing_Margin':
                curr_header = 'OPM'
            row_header.append(curr_header)

    # Transpose the data and filter out rows where all values are empty and year is within the last 10 years
    # transposed_data = [row for row in zip(headers, *rows) if
    #                    not all(value == '' for value in row) and row[0] >= current_year - 10]

    transposed_data = {header: [row[i] for row in rows if row[i] != ''] for i, header in enumerate(headers)}

    return row_header, transposed_data, doubt


def extract_yearly_data_from_soup(html, parse=False):
    if parse:
        soup = BeautifulSoup(html, "lxml")
    else:
        soup = html
    # Yearly Sections
    sections = {
        'Profit Loss': ('profit-loss', None),
        'Balanced Sheet': ('balance-sheet', None),
        'Cash Flow': ('cash-flow', None),
        'Ratios': ('ratios', None),
        'Shareholding Pattern Data': ('shareholding', 'yearly-shp')
    }

    section_data = dict()
    years = set()
    col_headers = []
    yearly_data = dict()
    doubt_list = []
    for section, args in sections.items():
        headers, data, doubt = extract_section_data(soup, *args)
        if section != 'Shareholding Pattern Data' and doubt:
            return None, None, doubt
        elif section == 'Shareholding Pattern Data':
            doubt = False
        doubt_list.append(doubt)
        section_data[section] = (headers, data)
        years.update(data.keys())
        col_headers.extend(headers)

    for year in years:
        temp = yearly_data.get(year, [])
        for section, info in section_data.items():
            headers, data = info
            default_list = [0] * len(headers)
            temp.extend(data.get(year, default_list))
        yearly_data[year] = temp

    return col_headers, yearly_data, any(doubt_list)
