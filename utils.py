def get_delta(current_value: int, previous_value: int | None) -> str | None:
    if previous_value is None:
        return None
    return f"{(current_value - previous_value):+,d}"


def stringify(path: str) -> str:
    return f"'{path}'"


def paths_for_years(start_year: int, end_year: int, url_template: str) -> list[str]:
    return [stringify(url_template.format(year=year)) for year in range(start_year, end_year + 1)]
