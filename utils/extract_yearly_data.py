import re
import sys
from bs4 import BeautifulSoup
from datetime import datetime
from utils.stockload import DBUtils


def find_section_table(soup, section_id, data_tab_id=None):
    """Retrieve a specific table within a section and tab in the HTML."""
    try:
        section = soup.find('section', id=section_id)
    except TypeError as e:
        print("Invalid soup structure", e)
        return None
    if not section:
        raise ValueError(f"Section with id '{section_id}' not found in HTML.")

    table_container = section.find('div', id=data_tab_id) if data_tab_id else section
    table = table_container.find('table', class_='data-table') if table_container else None
    if not table:
        raise ValueError(f"Table not found in section '{section_id}', tab '{data_tab_id}'.")

    return table


def extract_headers(table):
    """Extract and validate column headers from the table."""
    headers, ttm_flag = [], False
    year_pattern = re.compile(r'\b\d{4}\b')  # Pattern to match 4-digit years

    for header in table.find('thead').find_all('th'):
        text = header.get_text(strip=True)
        if text:
            # Extract year if header is in "Month Year" format or plain year format
            year_match = year_pattern.search(text)
            if year_match:
                year = int(year_match.group())
                headers.append(year)
            elif text == 'TTM':
                ttm_flag = True
            else:
                print(f"Unexpected header found: {text}")

    if ttm_flag and headers:
        headers.append(max(headers) + 1)

    current_year = datetime.now().year
    doubt = (current_year not in headers and current_year - 1 not in headers) or len(headers) < 2
    return headers, doubt


def extract_rows(table):
    """Extract rows and headers for each row from the table."""
    rows, row_headers = [], []
    for row in table.find('tbody').find_all('tr'):
        cells = [cell.get_text(strip=True).replace('+', '').replace('%', '').replace(',', '') for cell in
                 row.find_all('td')]
        row_data = [float(value) if value else 0 for value in cells[1:]]
        rows.append(row_data)

        row_header = cells[0].strip().replace(' ', '_').replace('.', '') if cells[0] else ''
        row_headers.append(map_headers(row_header))

    return row_headers, rows


def map_headers(header_name):
    """Standardize header names."""
    header_map = {
        'Revenue': 'Sales',
        'Financing_Profit': 'Operating_Profit',
        'Financing_Margin': 'OPM'
    }
    return header_map.get(header_name, header_name)


def extract_section_data(soup, section_id, data_tab_id=None):
    """Extract data from a section and return headers, rows, and any doubts about completeness."""
    table = find_section_table(soup, section_id, data_tab_id)
    if not table:
        return [], {}, True

    headers, doubt = extract_headers(table)
    row_headers, rows = extract_rows(table)

    transposed_data = {header: [row[i] for row in rows if row[i] != ''] for i, header in enumerate(headers)}
    return row_headers, transposed_data, doubt


def extract_yearly_data_from_soup(html, parse=False):
    """Extract yearly data across multiple sections and tabs in the HTML."""
    soup = BeautifulSoup(html, "lxml") if parse else html
    sections = {
        'Profit Loss': ('profit-loss', None),
        'Balanced Sheet': ('balance-sheet', None),
        'Cash Flow': ('cash-flow', None),
        'Ratios': ('ratios', None),
        'Shareholding Pattern Data': ('shareholding', 'yearly-shp')
    }

    section_data, years, col_headers, doubt_list = {}, set(), [], []
    for section, args in sections.items():
        headers, data, doubt = extract_section_data(soup, *args)
        if section != 'Shareholding Pattern Data' and doubt:
            print(f"Missing Section: {section}")
            return None, None, doubt

        section_data[section] = (headers, data)
        years.update(data.keys())
        col_headers.extend(headers)
        doubt_list.append(doubt)

    yearly_data = assemble_yearly_data(years, section_data)
    return col_headers, yearly_data, any(doubt_list)


def assemble_yearly_data(years, section_data):
    """Compile yearly data from all sections."""
    yearly_data = {}
    for year in years:
        year_values = []
        for headers, data in section_data.values():
            year_values.extend(data.get(year, [0] * len(headers)))
        yearly_data[year] = year_values
    return yearly_data
