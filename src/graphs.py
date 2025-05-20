import altair as alt
from src.data.data_process import DataIndex
import logging


class DataGraph(DataIndex):
    def format_money(self, val):
        abs_val = abs(val)
        sign = "-" if val < 0 else ""

        if abs_val >= 1e9:
            return f"{sign}${abs_val / 1e9:.1f}B"
        elif abs_val >= 1e6:
            return f"{sign}${abs_val / 1e6:.1f}M"
        elif abs_val >= 1e3:
            return f"{sign}${abs_val / 1e3:.1f}K"
        else:
            return f"{sign}${abs_val:.0f}"
    
    def create_spending_by_category_graph(self, year: int, quarter: int, month: int, type: str, category: str):
        process = DataIndex()
        df = process.process_awards_by_category(year, quarter, month, type, category)
        grouped_pd = df.to_pandas()
        grouped_pd['formatted_text'] = grouped_pd["federal_action_obligation"].apply(self.format_money)

        chart = alt.Chart(grouped_pd).mark_bar().encode(
            y=alt.Y(f'{category}:N', title=None, sort='-x'),
            x=alt.X(
                'federal_action_obligation:Q', 
                title=None,
                scale=alt.Scale(type='sqrt'),
                axis=None
            )
        )

        text = alt.Chart(grouped_pd).mark_text(
            baseline='middle',
            align=alt.ExprRef("datum.federal_action_obligation < 0 ? 'right' : 'left'"),
            dx=alt.ExprRef("datum.federal_action_obligation < 0 ? -3 : 3")
        ).encode(
            y=alt.Y(f'{category}:N', sort='-x'),
            x=alt.X('federal_action_obligation:Q'),
            text='formatted_text:N'
        )

        data_chart = chart + text

        return data_chart
    
    def create_secter_graph(self, type: str, secter: str):
        process = DataIndex()
        df = process.process_awards_by_secter(type, secter)
        grouped_pd = df.to_pandas()
        grouped_pd['formatted_text'] = grouped_pd["federal_action_obligation"].apply(self.format_money)

        if type == 'month':
            sort_expr = grouped_pd["parsed_period"].tolist()
        else:
            sort_expr = 'x'

        num_points = len(grouped_pd['time_period'].unique())
        chart_width = max(600, num_points * 15)

        data_chart = alt.Chart(grouped_pd).mark_line().encode(
            x=alt.X('time_period:O', title=None, sort=sort_expr),
            y=alt.Y('federal_action_obligation:Q', title=None)
        ).properties(width=chart_width)

        return data_chart