from dataclasses import dataclass
from typing import List, Optional
import math

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
    if value is None or math.isnan(value):
        return ValidationError(row_number=row_num, column=column, value=value, error_message="Value cannot be null")
    elif value < 0:
        return ValidationError(row_number=row_num, column=column, value=value, error_message=f"Value cannot be negative, got {value}")
    return None