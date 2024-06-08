import sys

from bs4 import BeautifulSoup
from datetime import datetime

from utils.stockload import DBUtils

def extract_section_data(soup, section_id, data_tab_id=None):
    current_year = datetime.now().year
    section = soup.find('section', id=section_id)
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

    headers = [int(header.get_text(strip=True).strip().split(' ')[-1]) for header in table.find('thead').find_all('th')
               if header.get_text(strip=True) != '']

    rows = []
    row_header = []
    for row in table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        row_data = [cell.get_text(strip=True).replace('+', '').replace('%', '').replace(',', '') for cell in cells]
        temp = [float(value) if value != '' else 0 for value in row_data[1:]]
        rows.append(temp)
        if row_data[0] != '':
            row_header.append(row_data[0].strip().replace(' ', '_'))

    # Transpose the data and filter out rows where all values are empty and year is within the last 10 years
    # transposed_data = [row for row in zip(headers, *rows) if
    #                    not all(value == '' for value in row) and row[0] >= current_year - 10]

    transposed_data = {header: [row[i] for row in rows if row[i] != ''] for i, header in enumerate(headers)}

    return row_header, transposed_data


def extract_yearly_data(html):
    soup = BeautifulSoup(html, "html.parser")
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

    for section, args in sections.items():
        headers, data = extract_section_data(soup, *args)
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

    return col_headers, yearly_data


def main():
    db_utils = DBUtils()
    soup_data = db_utils.get_soup_base()
    for record in soup_data:
        col_headers, yearly_data = extract_yearly_data(record[1])
        # db_utils.upsert_yearly_fundamentals(col_headers, yearly_data)


if __name__ == '__main__':
    main()
