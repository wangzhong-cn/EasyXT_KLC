from typing import Optional

import pandas as pd


class ChartDatafeed:
    def __init__(self):
        self._last_data: Optional[pd.DataFrame] = None

    def set_data(self, data: pd.DataFrame) -> None:
        self._last_data = data

    def last_data(self) -> Optional[pd.DataFrame]:
        return self._last_data
