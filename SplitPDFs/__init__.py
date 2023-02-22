import logging
import base64
import io
import PyPDF2
import json
import os
import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient


def main(myblob: func.InputStream):
    logging.info('Python Blob trigger function started.')

    logging.info(f"--- Python blob trigger function processed blob \n"
                 f"----- Name: {myblob.name}\n"
                 f"----- Blob Size: {myblob.length} bytes")

    
    # Convert file content to bytes
    blob_bytes = myblob.read()

    endpoint = "https://medicalclassification.cognitiveservices.azure.com/"
    key = "578e9544d03b4facbaacf2025fb58be2"

    document_analysis_client = DocumentAnalysisClient( endpoint=endpoint, credential=AzureKeyCredential(key) )
    text_analytics_client = TextAnalyticsClient(endpoint="https://customdocclass.cognitiveservices.azure.com/", credential=AzureKeyCredential("369c649184e7443b8fcb72d9a9fc9641"))

    connection_string = "DefaultEndpointsProtocol=https;AccountName=test9d40;AccountKey=VmVaBaq9MPu//AyWR/1o+q+BP1fidv4fdyuyHOSYAYybGbMNJIypAhslyd3eVdJL+xO4F+kG+2aR+AStEB6aTg==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Generate date time object of the run
    dt = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')

    # Extract original PDF file name
    pdf_prefix_file_name = ''.join(myblob.name.split('.pdf')[:1])
    pdf_prefix_file_name = pdf_prefix_file_name[14:]
    logging.info(f"File Prefix: {pdf_prefix_file_name}")

    # Establish HOME directory for writing/reading temporary files
    HOME_LOCAL_DIR = os.environ['HOME']
    logging.info(f"HOME_DATA_DIR: {HOME_LOCAL_DIR}")
    
    # Open multi-page PDF file
    with io.BytesIO(blob_bytes) as open_pdf_file:
        read_pdf = PyPDF2.PdfReader(open_pdf_file)

        logging.info(len(read_pdf.pages))

        # Extract each page and write out to individual files
        # pdf_list = []
        for i in range(len(read_pdf.pages)):
            output = PyPDF2.PdfWriter()
            output.add_page(read_pdf.pages[i])
            
            # Temporarily write PDF to disk
            temp_pdf_fn = pdf_prefix_file_name +'_'+ str(i + 1)+ str(".pdf")
            temp_pdf_fp = os.path.join(HOME_LOCAL_DIR, temp_pdf_fn)
            with open(temp_pdf_fp, "wb") as outputStream:
                output.write(outputStream)

            with open(temp_pdf_fp, 'rb') as temp_pdf_file:
                poller = document_analysis_client.begin_analyze_document(
                "prebuilt-read", document=f
            )
            result = poller.result()

            temp_text_fn = temp_pdf_fn +str(".txt")
            temp_text_fp = os.path.join(HOME_LOCAL_DIR, temp_text_fn)

            for page in result.pages:
                page_contents = ""
                for word in page.words:
                    page_contents += word.content
                    page_contents += " "

                    with open(temp_text_fp, "w") as f:
                        f.write(page_contents)

            with open(temp_text_fp) as fd:
                document = [fd.read()]

            poller = text_analytics_client.begin_single_label_classify(
                document,
                project_name="ClassifyMedical_Nonmedical",
                deployment_name="Language_Model"
            )

            document_results = poller.result()
            for doc, classification_result in zip(document, document_results):
                if classification_result.kind == "CustomDocumentClassification":
                    classification = classification_result.classifications[0]
                    logging.info("The document text '{}' was classified as '{}' with confidence score {}.".format(
                        doc, classification.category, classification.confidence_score)
                    )

                    if classification.category == 'Medical':
                        if classification.confidence_score>=0.5:
                            blob_client = blob_service_client.get_blob_client(container="medical-pdf-documents-single", blob=temp_pdf_fn)
                            # Read back in the PDF to get the bytes-like version
                            with open(temp_pdf_fp, 'rb') as temp_pdf_file:
                                blob_client.upload_blob(temp_pdf_file)
                        else:
                            blob_client = blob_service_client.get_blob_client(container="pdf-documents-single-review", blob=temp_pdf_fn)
                            # Read back in the PDF to get the bytes-like version
                            with open(temp_pdf_fp, 'rb') as temp_pdf_file:
                                blob_client.upload_blob(temp_pdf_file)
                    else:
                        if classification.confidence_score>=0.5:
                            blob_client = blob_service_client.get_blob_client(container="nonmedical-pdf-documents-single", blob=temp_pdf_fn)
                            # Read back in the PDF to get the bytes-like version
                            with open(temp_pdf_fp, 'rb') as temp_pdf_file:
                                blob_client.upload_blob(temp_pdf_file)
                        else:
                            blob_client = blob_service_client.get_blob_client(container="pdf-documents-single-review", blob=temp_pdf_fn)
                            # Read back in the PDF to get the bytes-like version
                            with open(temp_pdf_fp, 'rb') as temp_pdf_file:
                                blob_client.upload_blob(temp_pdf_file)
