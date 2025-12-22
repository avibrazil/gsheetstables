import logging
import datetime
import zoneinfo
import numbers
import json

import unidecode
import dotmap
import pandas
import google.oauth2.service_account
import googleapiclient.discovery

__version__="1.1"


class GSheetsTables():
    def __init__(self, gsheetid, service_account_file, column_rename_map=None, slugify=True):
        # Setup logging
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.gsheetid=gsheetid
        self.column_rename_map=column_rename_map
        self.slugify=slugify

        self._tables = dict()
        self._table_properties = dict()

        self.GoogleSheets = (
            googleapiclient.discovery.build(
                "sheets",
                "v4",
                credentials = (
                    google.oauth2.service_account
                    .Credentials.from_service_account_file(
                        service_account_file,
                        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
                    )
                )
            )
            .spreadsheets()
        )

        self.get_tables()



    def t(self,name):
        return self._tables[name]

    def p(self,name):
        return self._table_properties[name]

    @property
    def tables(self):
        return [
            t.name
            for t in self._t
        ]


    def get_tables(self):
        self._t=list()

        spreadsheet = dotmap.DotMap(
            self.GoogleSheets
            .get(
                spreadsheetId = self.gsheetid,
                includeGridData = False
            )
            .execute()
        )

        # Discover tables in spreadsheet
        for sheet in spreadsheet.sheets:
            if 'tables' in sheet:
                for table in sheet.tables:
                    self.logger.debug(f"Discovered table {table.name} with columns {table.columnProperties} and range {table.range}")

                    self._t.append(
                        dotmap.DotMap(
                            name    = table.name,
                            id      = table.tableId,
                            columns = table.columnProperties,
                            sheet   = sheet.properties.title,
                            raw_range = table.range,
                            range   = GSheetsTables.R1C1(table.range),
                        )
                    )

        # Retrieve raw table data
        result = dotmap.DotMap(
            self.GoogleSheets
            .values()
            .batchGet(
                spreadsheetId=self.gsheetid,
                ranges=[
                    f"{t.sheet}!{t.range}"
                    for t in self._t
                ],
                valueRenderOption="UNFORMATTED_VALUE"
            )
            .execute()
        )

        # results to pandas.DataFrames and convert values
        for i in range(len(self._t)):
            values=result.valueRanges[i]['values']

            columns=values[0]
            ncolumns=len(columns)
            data_rows=values[1:]

            self.logger.info(f"Working on «{self._t[i].name}» with {ncolumns} columns: {columns}")

            self.logger.debug(f"Raw data sample: {data_rows[0:4]}")

            # Google API frequently send columns outside the table, but
            # sometimes not. We can't rely on it so we normalize the shape of
            # our data.
            data_rows = [
                row[:ncolumns] + [None] * max(0, ncolumns - len(row))
                for row in data_rows
            ]

            self._t[i].data=(
                pandas.DataFrame(
                    columns=columns,
                    data=data_rows,
                    index=pandas.RangeIndex(
                        name  = '_GSheet_row',
                        start = self._t[i].raw_range.startRowIndex+2,
                        stop  = self._t[i].raw_range.startRowIndex+2 + len(data_rows),
                    )
                )
                .dropna(how='all')
            )

            # Convert Google Sheets date numerical data to real datetime
            for c in self._t[i].columns:
                if str(c.columnType) in {'DATE', 'DATE_TIME', 'TIME'}:
                    try:
                        self._t[i].data[c.columnName] = (
                            self._t[i].data[c.columnName].apply(
                                lambda serial: (
                                    (
                                        datetime.datetime(1899, 12, 30) +
                                        datetime.timedelta(days=serial)
                                    )
                                    .replace(tzinfo=zoneinfo.ZoneInfo(spreadsheet.properties.timeZone))
                                    if isinstance(serial, numbers.Number) and not pandas.isna(serial) else pandas.NaT
                                )
                            )
                        )
                    except Exception:
                        self.logger.exception(f"Sheet «{self._t[i].sheet}», table «{self._t[i].name}», column «{c.columnName}» has invalid date and time data")
                        raise

            renamer=dict()

            if self.slugify:
                renamer={
                    c.columnName: GSheetsTables.slugification(c.columnName)
                    for c in self._t[i].columns
                }

            if self.column_rename_map is not None and self._t[i].name in self.column_rename_map:
                renamer.update(self.column_rename_map[self._t[i].name])

            if len(renamer) > 0:
                self._t[i].data=self._t[i].data.rename(columns=renamer)

            self._tables[self._t[i].name] = self._t[i].data
            self._table_properties[self._t[i].name] = self._t[i]



    def slugification(name):
        return (
            unidecode.unidecode(name)
            .replace('/'  , '_')
            .replace(': ' , '_')
            .replace(':'  , '_')
            .replace(' '  , '_')
            .lower()
        )



    def colmap(self, JSON=True):
        colmap = {
            self._t[i].name: {
                self._t[i].columns[ic].columnName: self._t[i].data.columns[ic]
                for ic in range(len(self._t[i].columns))
            }
            for i in range(len(self._t))
        }

        if JSON:
            return json.dumps(colmap, ensure_ascii=False, indent=4)
        else:
            return colmap



    def R1C1(trange):
        return "R[{r.startRowIndex}]C[{r.startColumnIndex}]:R[{r.endRowIndex}]C[{r.endColumnIndex}]".format(r=trange)


