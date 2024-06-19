import base64
import secrets

from pathlib import Path

from pydantic import BaseModel

from typing import Optional, Annotated
import signal  # Add this import to handle signal
from litestar import MediaType, Request, Litestar, Controller, Response, post, get  # Importing Litestar
import traceback
import uvicorn

import os
import shutil
import requests
import sys

from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR
from litestar.datastructures import UploadFile
from litestar.enums import RequestEncodingType
from litestar.params import Body


from .server_utils import init_models_and_workers, process_single_pdf

def rand_string() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(8)).decode()
class BaseMarkerCliInput(BaseModel):
    in_folder: str
    out_folder: str 
    chunk_idx: int = 0
    num_chunks : int = 1
    max_pdfs : Optional[int] = None 
    min_length : Optional[int] = None 
    metadata_file : Optional[str] = None

class PDFUploadFormData(BaseModel):
    file: bytes

class URLUpload(BaseModel):
    url: str
class PathUpload(BaseModel):
    path: str

TMP_DIR = Path("/tmp")
MARKER_TMP_DIR = TMP_DIR / Path("marker")
class PDFProcessor(Controller):

    @get("/test_pdf_processing")
    async def test_pdf(self, request: Request ) -> str:
        print("This a test message")
        print("This is a test message sent to stderr",sys.stderr)
        doc_dir = MARKER_TMP_DIR / Path(rand_string())
        input_directory = doc_dir / Path("in")
        os.makedirs(input_directory, exist_ok=True)
        return await self.process_pdf_from_given_docdir(Path(data.path))
    @post("/process_pdf_from_file_path")
    async def process_pdf_from_file_path(self,data : URLUpload, request: Request ) -> str:
        print("This a test message")
        print("This is a test message sent to stderr",sys.stderr)
        doc_dir = MARKER_TMP_DIR / Path(rand_string())
        input_directory = doc_dir / Path("in")
        os.makedirs(input_directory, exist_ok=True)
        shutil.copy(data.path, input_directory)
        return await self.process_pdf_from_given_docdir(Path(data.path))

    async def process_pdf_from_given_docdir(self,doc_dir : Path) -> str:
        print(f"Called function on {doc_dir}")
        input_directory = doc_dir / Path("in")
        output_directory = doc_dir / Path("out")
        
        # Ensure the directories exist
        os.makedirs(input_directory, exist_ok=True)

        os.makedirs(output_directory, exist_ok=True)


        def get_pdf_files(pdf_path: Path) -> list[Path]:
            if not pdf_path.is_dir():
                raise ValueError("Path is not a directory")
            return [f for f in pdf_path.iterdir() if f.is_file() and f.suffix == '.pdf']
        print("trying to locate pdf file")
        pdf_list = get_pdf_files(input_directory)
        if len(pdf_list) == 0 :
            print(f"No PDF's found in input directory : {input_directory}")
            print(f"{input_directory.iterdir()}")
            return ""
        first_pdf_filepath = pdf_list[0]
        print(f"found pdf at: {first_pdf_filepath}")
        process_single_pdf(first_pdf_filepath,output_directory)
        
        print("Successfully processed pdf.")
        # Read the output markdown file
        # TODO : Fix at some point with tests
        def pdf_to_md_path(pdf_path: Path) -> Path:
            return (pdf_path.parent).parent / Path(f'out/{pdf_path.stem}/{pdf_path.stem}.md')
        output_filename = pdf_to_md_path(first_pdf_filepath)
        if not os.path.exists(output_filename):
            return f"Output markdown file not found at : {output_filename}"
        with open(output_filename, "r") as f:
            markdown_content = f.read()
        # Cleanup directories
        shutil.rmtree(doc_dir)
        # Return the markdown content as a response
        return markdown_content 

    @post("/process_pdf_from_url")
    async def process_pdf_from_url(self,data : URLUpload ) -> str:
        print("This a test message")
        def download_file(url: str, savedir: Path,extension : Optional[str]=None) -> Path:
            if extension is None:
                extension = ""
            # TODO: Use a temporary directory for downloads or archive it in some other way.
            local_filename = savedir / Path(rand_string()+extension)
            # NOTE the stream=True parameter below
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        f.write(chunk)
            return local_filename
        doc_dir = MARKER_TMP_DIR / Path(rand_string())
        print(f"This a test message: {doc_dir}")

        input_directory = doc_dir / Path("in")
        os.makedirs(input_directory, exist_ok=True)
        print(f"This a test message: {input_directory}")
        download_file(data.url,input_directory,".pdf")
        return await self.process_pdf_from_given_docdir(doc_dir)
    @post(path="/test-upload", )
    async def handle_file_upload(self,
        data: Annotated[UploadFile, Body(media_type=RequestEncodingType.MULTI_PART)],
    ) -> str:
        content = await data.read()
        filename = data.filename
        return f"{filename}, {content.decode()}"
        
    @post("/process_pdf_upload", media_type=MediaType.TEXT)
    async def process_pdf_upload(self,
        data: Annotated[UploadFile, Body(media_type=RequestEncodingType.MULTI_PART)],
    ) -> str:
        doc_dir = MARKER_TMP_DIR / Path(rand_string())
        # Parse the uploaded file
        pdf_binary = data.file
        input_directory = doc_dir / Path("in")
        # Ensure the directories exist
        os.makedirs(input_directory, exist_ok=True)
        # Save the PDF to the output directory
        pdf_filename = input_directory / Path(rand_string() + ".pdf")
        with open(pdf_filename, "wb") as f:
            f.write(pdf_binary)
        return await self.process_pdf_from_given_docdir(doc_dir)


    @post("/process_pdfs_raw_cli")
    async def process_pdfs_endpoint_raw_cli(self,data: BaseMarkerCliInput) -> None:
        in_folder = data.in_folder
        out_folder = data.out_folder
        chunk_idx = data.chunk_idx
        num_chunks = data.num_chunks
        max_pdfs = data.max_pdfs
        min_length = data.min_length
        metadata_file = data.metadata_file

        result = process_pdfs_core(in_folder, out_folder, chunk_idx, num_chunks, max_pdfs, min_length, metadata_file)


def plain_text_exception_handler(request: Request, exc: Exception) -> Response:
    """Default handler for exceptions subclassed from HTTPException."""
    tb = traceback.format_exc()
    status_code = getattr(exc, "status_code", HTTP_500_INTERNAL_SERVER_ERROR)
    detail = getattr(exc, "detail", "")

    return Response(
        media_type=MediaType.TEXT,
        content=tb,
        status_code=status_code,
    )

def start_server():
    init_models_and_workers(workers=5)  # Initialize models and workers with a default worker count of 5
    port = os.environ.get("MARKER_PORT")
    if port is None:
        port = 2718
    app = Litestar(
        route_handlers = [PDFProcessor],
        exception_handlers={Exception: plain_text_exception_handler},
    )

    run_config = uvicorn.Config(app, port=port, host="0.0.0.0")
    server = uvicorn.Server(run_config)
    signal.signal(signal.SIGINT, lambda s, f: shutdown())  # Updated to catch Control-C and run shutdown
    server.run()
    shutdown()


def shutdown():
    global model_refs, pool
    if pool:
        pool.close()
        pool.join()
    del model_refs


if __name__ == "__main__":
    start_server()



