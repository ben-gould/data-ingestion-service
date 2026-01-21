from dataclasses import dataclass
from typing import List, Optional, Callable, Dict
import math
import pandas as pd
from datetime import datetime, date, time


@dataclass
class ValidationError:
    row_number: int
    column: str
    value: any
    error_message: str

@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[ValidationError]

    def add_error(self, row_num: int, col: str, val: any, message: str) -> None:
        self.errors.append(ValidationError(row_number=row_num, 
                                           column=col,
                                           value=val,
                                           error_message=message))
        self.is_valid = False

def validate_positive(value: float, row_num: int, column: str) -> Optional[ValidationError]:
    """Validate that a numeric value is positive."""
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ValidationError(row_num, column, value, f"Cannot convert '{value}' to number")
    if numeric_value is None or (isinstance(numeric_value, float) and pd.isna(numeric_value)):
        return ValidationError(row_number=row_num, column=column, value=value, error_message="Value cannot be null")
    elif numeric_value < 0:
        return ValidationError(row_number=row_num, column=column, value=value, error_message=f"Value cannot be negative, got {value}")
    return None

def validate_date_not_future(value: str, row_num: int, column: str) -> Optional[ValidationError]:
    try:
        date = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return ValidationError(f"Invalid date format in row {row_num}: {value}")

    if date > datetime.now():
        return ValidationError(f"Date cannot be in the future, got {value} in row {row_num}")
    return None

def validate_not_null(value: any, row_num: int, column: str) -> Optional[ValidationError]:
    if value is None or (isinstance(value, str) and value.strip == "") or (isinstance(value, float) and math.isnan(value)):
        return ValidationError(f"Value in row {row_num}, column {column} cannot be null")
    return None

class DataValidator:
    def __init__(self):
        # maps column names to list of validation functions
        self.rules: Dict[str : List[Callable]] = {}

    def add_rule(self, column: str, validation_func: Callable):
        if column not in self.rules:
            self.rules[column] = []
        self.rules[column].append(validation_func)
        return self
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        result = ValidationResult(is_valid=True, errors = [])
        
        for row_num, row in df.iterrows():
            for column, validators in self.rules.items():
                if column not in df.columns:
                    result.add_error(row_num=row_num, col=column, val=None, message=f"Required column {column} is missing")
                
                    continue 
                value = row[column]
                for validator in validators:
                    error = validator(value, row_num, column)
                    if error:
                        result.add_error(error.row_number, error.column, 
                                       error.value, error.error_message)


        return result 
    
