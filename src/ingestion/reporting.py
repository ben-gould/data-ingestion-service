import pandas as pd
from ingestion.validation import ValidationResult
import matplotlib.pyplot as plt
import base64
from io import BytesIO
from pathlib import Path

def calculate_data_quality_metrics(df: pd.DataFrame) -> dict:
    'Calculate basic statistics for each column'
    'Im going to assume that the schema holds for df; hardcoded'

    metrics = {}
    
    # count nulls for every column
    for col in df.columns:
        null_count = df[col].isnull().sum()
        metrics[col] = {
            'null_count' : null_count,
            'null_pct' : (null_count/len(df)) * 100
        }
    
    # calculate numerical statistics for amount
    if 'amount' in df.columns:
        valid_amounts = pd.to_numeric(df['amount'], errors='coerce').dropna()
    
        metrics['amount'].update({
            'min': valid_amounts.min(),
            'max': valid_amounts.max(),
            'mean': valid_amounts.mean(),
            'median': valid_amounts.median()
        })

    # calculate value counts for user id's and currencies
    for col in ['user_id', 'currency']:
        metrics[col]['value_counts'] = df[col].value_counts().to_dict()

    return metrics
    

def summarize_errors(validation_result: ValidationResult) -> dict:
    'Count errors by type'
    error_breakdown = {}

    errors = validation_result.errors
    for error in errors:
        if error.error_message.endswith('cannot be null'):
            error_type = "missing_values"
        elif error.error_message.startswith('Value cannot be negative'):
            error_type = "negative_amount"
        elif "date" in error.error_message.lower():
            error_type = "invalid_date"
        else:
            error_type = "other errors"
        error_breakdown[error_type] = error_breakdown.get(error_type, 0) + 1
    
    return error_breakdown
    

def create_error_chart(error_breakdown):
    """Create pie chart of error types, return as base64."""
    fig, ax = plt.subplots()
    ax.pie(error_breakdown.values(), labels=error_breakdown.keys(), autopct='%1.1f%%')
    
    # Convert to base64 for HTML embedding
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()
    
    return f"data:image/png;base64,{image_base64}"

def generate_metrics_rows(metrics):
    """Convert metrics dict to HTML table rows."""
    rows = []
    for column, stats in metrics.items():
        null_pct = stats.get('null_pct', 'N/A')
        min_val = stats.get('min', 'N/A')
        max_val = stats.get('max', 'N/A')
        mean_val = stats.get('mean', 'N/A')
        
        row = f"<tr><td>{column}</td><td>{null_pct:.1f}%</td><td>{min_val}</td><td>{max_val}</td><td>{mean_val}</td></tr>"
        rows.append(row)
    
    return '\n'.join(rows)

def render_html_template(metrics, charts, summary):
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Quality Report</title>
        <style>
            body {{ font-family: Arial; margin: 40px; }}
            .summary {{ background: #f0f0f0; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        </style>
    </head>
    <body>
        <h1>Data Quality Report</h1>
        
        <div class="summary">
            <h2>Summary</h2>
            <p>Total Rows: {summary['total_rows']}</p>
            <p>Valid: {summary['valid_rows']} ({summary['valid_rows']/summary['total_rows']*100:.1f}%)</p>
            <p>Invalid: {summary['invalid_rows']} ({summary['invalid_rows']/summary['total_rows']*100:.1f}%)</p>
        </div>
        
        <h2>Error Breakdown</h2>
        <img src="{charts['error_types']}" width="600">
        
        <h2>Data Quality Metrics</h2>
        <table>
            <tr><th>Column</th><th>Null %</th><th>Min</th><th>Max</th><th>Mean</th></tr>
            {generate_metrics_rows(metrics)}
        </table>
    </body>
    </html>
    """

def generate_quality_report(df, validation_result, summary, output_dir=Path('reports')):
    metrics = calculate_data_quality_metrics(df) 
    error_breakdown = summarize_errors(validation_result) 
    charts = {'error_types': create_error_chart(error_breakdown)} 
    html = render_html_template(metrics, charts, summary) 

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / 'data_quality_report.html'
    
    with open(report_path, 'w') as f:
        f.write(html)

    return str(report_path)