import os
import pandas as pd
import clickhouse_connect
import uuid
from datetime import datetime
from flask import Flask, render_template, request, flash, redirect, url_for
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'default_secret_key')

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Columns required to be present in the Excel file
EXCEL_REQUIRED_COLUMNS = [
    'coa_code',
    'unit_code',
    'periode_thn',
    'amount',
    'periode_bulan',
    'periode_hari',
    'target_untuk_tanggal'
]

# Create upload folder if not exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv('CH_HOST', 'localhost'),
            port=int(os.getenv('CH_PORT', 8123)),
            username=os.getenv('CH_USER', 'default'),
            password=os.getenv('CH_PASSWORD', ''),
            database=os.getenv('CH_DATABASE', 'default')
        )
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    # 1. Authentication check
    username = request.form.get('username')
    password = request.form.get('password')
    
    expected_user = os.getenv('APP_USERNAME', 'admin')
    expected_pass_hash = os.getenv('APP_PASSWORD_HASH')
    
    if not expected_pass_hash:
        plain_pass = os.getenv('APP_PASSWORD', 'password123')
        if username != expected_user or password != plain_pass:
            flash('Invalid Username or Password!', 'danger')
            return redirect(url_for('index'))
    else:
        if username != expected_user or not check_password_hash(expected_pass_hash, password):
            flash('Invalid Username or Password!', 'danger')
            return redirect(url_for('index'))

    # File check
    if 'file' not in request.files:
        flash('No file part', 'danger')
        return redirect(url_for('index'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No selected file', 'danger')
        return redirect(url_for('index'))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            # 1. Read Excel using Pandas
            df = pd.read_excel(filepath)
            
            # Normalize column names (lower case, trim spaces)
            df.columns = [str(c).strip().lower() for c in df.columns]
            
            # 2. Validate Structure (Only check business columns)
            missing_cols = [col for col in EXCEL_REQUIRED_COLUMNS if col not in df.columns]
            if missing_cols:
                flash(f'Validation Failed! Excel is missing: {", ".join(missing_cols)}', 'danger')
                return redirect(url_for('index'))
            
            # 3. Auto-generate System Columns
            df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
            df['create_at'] = datetime.now()
            df['create_by'] = username 
            
            # 4. Transform: Data Type Conversion
            try:
                df['target_untuk_tanggal'] = pd.to_datetime(df['target_untuk_tanggal']).dt.date
                df['periode_thn'] = pd.to_numeric(df['periode_thn']).astype('int32')
                df['amount'] = pd.to_numeric(df['amount']).astype('int64')
                df['periode_bulan'] = pd.to_numeric(df['periode_bulan']).astype('uint8')
                df['periode_hari'] = pd.to_numeric(df['periode_hari']).astype('uint8')
                df['coa_code'] = df['coa_code'].astype(str)
                df['unit_code'] = df['unit_code'].astype(str)
                
            except Exception as type_err:
                flash(f'Data Type Error in Excel data: {str(type_err)}', 'danger')
                return redirect(url_for('index'))

            # Define final column order matching the database schema
            final_columns = [
                'id', 'coa_code', 'unit_code', 'periode_thn', 'amount', 
                'create_at', 'create_by', 'periode_bulan', 'periode_hari', 'target_untuk_tanggal'
            ]
            df = df[final_columns]

            # 5. Load: Insert into ClickHouse
            client = get_clickhouse_client()
            if not client:
                flash('Could not connect to ClickHouse database!', 'danger')
                return redirect(url_for('index'))
            
            table_name = "dwh_transaksi_target_berjalan"
            client.insert_df(table_name, df)
            
            flash(f'Success! {len(df)} rows uploaded to table "{table_name}".', 'success')
            
        except Exception as e:
            flash(f'Error processing file: {str(e)}', 'danger')
        finally:
            # Cleanup
            if os.path.exists(filepath):
                os.remove(filepath)
                
        return redirect(url_for('index'))
    else:
        flash('Allowed file types are .xlsx, .xls', 'danger')
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
