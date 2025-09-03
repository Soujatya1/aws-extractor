import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import pandas as pd
from datetime import datetime
import io
import zipfile
import re

# Configure Streamlit page
st.set_page_config(
    page_title="S3 PDF Extractor by Name",
    page_icon="📄",
    layout="wide"
)

st.title("📄 S3 PDF Extractor - Extract Specific PDFs by Name")
st.markdown("Extract specific PDF files from your S3 bucket by entering exact filenames or patterns")

# Sidebar for AWS configuration
st.sidebar.header("AWS Configuration")

# AWS Credentials input
aws_access_key = st.sidebar.text_input("AWS Access Key ID", type="password")
aws_secret_key = st.sidebar.text_input("AWS Secret Access Key", type="password")
aws_region = st.sidebar.selectbox("AWS Region", [
    "ap-south-1"
])

# S3 Bucket configuration
bucket_name = st.sidebar.text_input("S3 Bucket Name")
prefix = st.sidebar.text_input("Folder Prefix (optional)", help="Leave empty to search entire bucket")

def get_s3_client():
    """Create and return S3 client"""
    try:
        if aws_access_key and aws_secret_key:
            return boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            # Try to use default credentials (IAM role, AWS CLI config, etc.)
            return boto3.client('s3', region_name=aws_region)
    except Exception as e:
        st.error(f"Error creating S3 client: {str(e)}")
        return None

def find_specific_pdfs(s3_client, bucket, target_files, prefix=""):
    """Find specific PDF files in the S3 bucket"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        found_files = []
        all_s3_files = []
        
        # Get all PDF files in bucket
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].lower().endswith('.pdf'):
                        all_s3_files.append({
                            'Key': obj['Key'],
                            'FileName': obj['Key'].split('/')[-1],
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified'],
                            'SizeFormatted': format_file_size(obj['Size'])
                        })
        
        # Match target files with S3 files
        for target_file in target_files:
            target_file = target_file.strip()
            if not target_file:
                continue
                
            # Try exact filename match first
            exact_matches = [f for f in all_s3_files if f['FileName'].lower() == target_file.lower()]
            
            if exact_matches:
                found_files.extend(exact_matches)
            else:
                # Try partial match
                partial_matches = [f for f in all_s3_files if target_file.lower() in f['FileName'].lower()]
                if partial_matches:
                    found_files.extend(partial_matches)
        
        return found_files, all_s3_files
        
    except ClientError as e:
        st.error(f"Error accessing S3 bucket: {str(e)}")
        return [], []
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return [], []

def search_by_pattern(s3_client, bucket, pattern, prefix=""):
    """Search PDF files using regex pattern"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        found_files = []
        pattern_regex = re.compile(pattern, re.IGNORECASE)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].lower().endswith('.pdf'):
                        if pattern_regex.search(obj['Key']) or pattern_regex.search(obj['Key'].split('/')[-1]):
                            found_files.append({
                                'Key': obj['Key'],
                                'FileName': obj['Key'].split('/')[-1],
                                'Size': obj['Size'],
                                'LastModified': obj['LastModified'],
                                'SizeFormatted': format_file_size(obj['Size'])
                            })
        
        return found_files
        
    except re.error as e:
        st.error(f"Invalid regex pattern: {e}")
        return []
    except Exception as e:
        st.error(f"Error searching files: {str(e)}")
        return []

def format_file_size(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.1f} {size_names[i]}"

def download_file(s3_client, bucket, key):
    """Download a file from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        st.error(f"Error downloading {key}: {str(e)}")
        return None

def create_zip_file(files_data):
    """Create a ZIP file containing multiple PDFs"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_name, file_data in files_data.items():
            # Use just the filename without the full path for cleaner zip structure
            clean_name = file_name.split('/')[-1]
            zip_file.writestr(clean_name, file_data)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# Main application
st.header("Specify PDFs to Extract")

# Method selection
extraction_method = st.radio(
    "Choose extraction method:",
    ["Specific Filenames", "Pattern Search", "Browse All PDFs"]
)

if extraction_method == "Specific Filenames":
    st.subheader("📝 Enter Specific PDF Filenames")
    
    # Input methods
    input_method = st.radio("Input method:", ["Text Area", "File Upload"])
    
    target_files = []
    if input_method == "Text Area":
        filenames_input = st.text_area(
            "Enter PDF filenames (one per line):",
            placeholder="report_2024.pdf\ninvoice_001.pdf\ndocument_final.pdf\nanalysis_report.pdf",
            height=150,
            help="Enter exact filenames including .pdf extension"
        )
        if filenames_input:
            target_files = [name.strip() for name in filenames_input.split('\n') if name.strip()]
    
    else:  # File Upload
        uploaded_file = st.file_uploader(
            "Upload a text file with PDF names (one per line):",
            type=['txt']
        )
        if uploaded_file:
            content = str(uploaded_file.read(), "utf-8")
            target_files = [name.strip() for name in content.split('\n') if name.strip()]
            st.info(f"Loaded {len(target_files)} filenames from uploaded file")

elif extraction_method == "Pattern Search":
    st.subheader("🔍 Search by Pattern")
    
    pattern_type = st.selectbox("Pattern type:", ["Contains text", "Starts with", "Ends with", "Regex"])
    
    if pattern_type == "Contains text":
        search_text = st.text_input("Enter text that filename should contain:", placeholder="report")
        pattern = f".*{re.escape(search_text)}.*" if search_text else ""
    elif pattern_type == "Starts with":
        start_text = st.text_input("Enter text that filename should start with:", placeholder="invoice_")
        pattern = f"^{re.escape(start_text)}.*" if start_text else ""
    elif pattern_type == "Ends with":
        end_text = st.text_input("Enter text that filename should end with (before .pdf):", placeholder="_final")
        pattern = f".*{re.escape(end_text)}\.pdf$" if end_text else ""
    else:  # Regex
        pattern = st.text_input(
            "Enter regex pattern:",
            placeholder="report_\\d{4}.*\\.pdf",
            help="Use regular expressions for complex matching"
        )
    
    # Show pattern preview
    if pattern:
        st.code(f"Pattern: {pattern}")

# Connection and search
if bucket_name and (extraction_method == "Browse All PDFs" or 
                   (extraction_method == "Specific Filenames" and target_files) or 
                   (extraction_method == "Pattern Search" and pattern)):
    
    if st.button("🔍 Search for PDFs"):
        with st.spinner("Searching for PDFs..."):
            s3_client = get_s3_client()
            if s3_client:
                if extraction_method == "Specific Filenames":
                    found_files, all_files = find_specific_pdfs(s3_client, bucket_name, target_files, prefix)
                    
                    # Show search results
                    st.subheader("Search Results")
                    if found_files:
                        st.success(f"Found {len(found_files)} matching PDFs out of {len(target_files)} requested")
                        
                        # Show which files were found and which weren't
                        found_names = {f['FileName'].lower() for f in found_files}
                        target_names_lower = {name.lower() for name in target_files}
                        missing_files = target_names_lower - found_names
                        
                        if missing_files:
                            st.warning(f"Could not find {len(missing_files)} files:")
                            for missing in missing_files:
                                st.write(f"❌ {missing}")
                        
                        st.session_state.found_files = found_files
                    else:
                        st.error("No matching PDFs found")
                        
                        # Show suggestions
                        if all_files:
                            st.info("Available PDFs in bucket:")
                            for file_info in all_files[:10]:  # Show first 10
                                st.write(f"📄 {file_info['FileName']}")
                            if len(all_files) > 10:
                                st.write(f"... and {len(all_files) - 10} more files")
                
                elif extraction_method == "Pattern Search":
                    found_files = search_by_pattern(s3_client, bucket_name, pattern, prefix)
                    
                    st.subheader("Search Results")
                    if found_files:
                        st.success(f"Found {len(found_files)} PDFs matching pattern")
                        st.session_state.found_files = found_files
                    else:
                        st.error("No PDFs found matching the pattern")
                
                else:  # Browse All PDFs
                    # Reuse the pattern search with match-all pattern
                    found_files = search_by_pattern(s3_client, bucket_name, ".*", prefix)
                    st.success(f"Found {len(found_files)} PDFs in bucket")
                    st.session_state.found_files = found_files
                
                st.session_state.s3_client = s3_client
                st.session_state.bucket_name = bucket_name

# Display found files and download options
if 'found_files' in st.session_state and st.session_state.found_files:
    st.header("📄 Found PDFs")
    
    # Create DataFrame for display
    df = pd.DataFrame(st.session_state.found_files)
    
    # Sort options
    sort_by = st.selectbox("Sort by:", ["Name", "Size", "Last Modified"])
    if sort_by == "Name":
        df = df.sort_values('FileName')
    elif sort_by == "Size":
        df = df.sort_values('Size', ascending=False)
    else:
        df = df.sort_values('LastModified', ascending=False)
    
    # File selection
    st.subheader("Select files to download:")
    
    # Quick selection buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("✅ Select All"):
            st.session_state.select_all = True
    with col2:
        if st.button("❌ Select None"):
            st.session_state.select_all = False
    with col3:
        st.metric("Total Files", len(df))
    
    # Individual file selection
    selected_files = []
    for idx, row in df.iterrows():
        default_value = getattr(st.session_state, 'select_all', False)
        col1, col2, col3 = st.columns([1, 3, 1])
        
        with col1:
            selected = st.checkbox("", key=f"select_{idx}", value=default_value)
        
        with col2:
            st.write(f"📄 **{row['FileName']}**")
            st.caption(f"Path: {row['Key']} | Size: {row['SizeFormatted']} | Modified: {row['LastModified'].strftime('%Y-%m-%d %H:%M')}")
        
        with col3:
            # Individual download button
            if st.button("⬇️", key=f"download_{idx}", help="Download this file"):
                with st.spinner(f"Downloading {row['FileName']}..."):
                    file_data = download_file(st.session_state.s3_client, st.session_state.bucket_name, row['Key'])
                    if file_data:
                        st.download_button(
                            label=f"💾 Save {row['FileName']}",
                            data=file_data,
                            file_name=row['FileName'],
                            mime="application/pdf",
                            key=f"save_{idx}"
                        )
        
        if selected:
            selected_files.append(row)
    
    # Bulk download options
    if selected_files:
        st.header("📦 Bulk Download")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Selected Files", len(selected_files))
            total_size = sum([f['Size'] for f in selected_files])
            st.metric("Total Size", format_file_size(total_size))
        
        with col2:
            if st.button("📦 Download Selected as ZIP", type="primary"):
                with st.spinner("Creating ZIP file..."):
                    files_data = {}
                    progress_bar = st.progress(0)
                    
                    for i, file_info in enumerate(selected_files):
                        file_data = download_file(
                            st.session_state.s3_client,
                            st.session_state.bucket_name,
                            file_info['Key']
                        )
                        if file_data:
                            files_data[file_info['FileName']] = file_data
                        
                        progress_bar.progress((i + 1) / len(selected_files))
                    
                    if files_data:
                        zip_data = create_zip_file(files_data)
                        st.download_button(
                            label="💾 Download ZIP File",
                            data=zip_data,
                            file_name=f"extracted_pdfs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                            mime="application/zip"
                        )
                        st.success(f"✅ ZIP file ready with {len(files_data)} PDFs!")

# Instructions
with st.expander("📖 How to Use"):
    st.markdown("""
    ### Step-by-step Instructions:
    
    1. **Configure AWS**: Enter your AWS credentials in the sidebar
    2. **Set Bucket**: Specify your S3 bucket name and optional folder prefix
    3. **Choose Method**:
       - **Specific Filenames**: Enter exact PDF names you want to extract
       - **Pattern Search**: Use patterns to find PDFs (e.g., all reports from 2024)
       - **Browse All PDFs**: See all PDFs in the bucket
    4. **Search**: Click "Search for PDFs" to find matching files
    5. **Select & Download**: Choose files and download individually or as ZIP
    
    ### Examples of Specific Filenames:
    ```
    annual_report_2024.pdf
    invoice_INV001.pdf
    project_summary.pdf
    financial_statement.pdf
    ```
    
    ### Pattern Search Examples:
    - **Contains "report"**: Finds all PDFs with "report" in filename
    - **Starts with "invoice_"**: Finds invoice_001.pdf, invoice_002.pdf, etc.
    - **Ends with "_final"**: Finds document_final.pdf, report_final.pdf, etc.
    - **Regex pattern**: `report_\\d{4}.*\\.pdf` finds report_2024_q1.pdf, etc.
    """)

# Footer
st.markdown("---")
st.markdown("🚀 Built with Streamlit • AWS SDK for Python (Boto3)")
