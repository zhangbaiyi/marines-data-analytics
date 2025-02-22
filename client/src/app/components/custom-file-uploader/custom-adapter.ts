import { ModelSignal } from "@angular/core";
import {
  FilePickerAdapter,
  FilePickerComponent,
  FilePreviewModel,
  UploadResponse,
  UploadStatus
} from "ngx-awesome-uploader";
import { Observable, of } from "rxjs";

type RemoveFileStatus = { success: boolean };

export class CustomAdapter extends FilePickerAdapter {
  private readonly FORM_DATA_IDENTIFIER = "files";

  constructor(
    private readonly formData: ModelSignal<FormData>,
    private readonly allowedFileExtensions: readonly string[],
    private readonly filePickerRef: FilePickerComponent
  ) {
    super();
  }

  private throwUploadStatusError(fileItem: FilePreviewModel, errorMsg: string): Observable<UploadResponse> {
    // eslint-disable-next-line no-console
    console.error(errorMsg);
    alert(errorMsg);
    this.deleteItemFromFormData(fileItem);
    this.filePickerRef.removeFile(fileItem);
    return of({ body: `[FAILURE] ${errorMsg}`, status: UploadStatus.ERROR });
  }

  private deleteItemFromFormData(fileItemToDelete: FilePreviewModel) {
    this.formData.update((prevFormData) => {
      const currFormDataFiles = new Set(prevFormData.getAll(this.FORM_DATA_IDENTIFIER));
      const isFileExisting = [...currFormDataFiles].some((entry) => (entry as File).name === fileItemToDelete.fileName);
      if (!isFileExisting) {
        return prevFormData;
      }

      const matchedEntryToDelete = [...currFormDataFiles].find(
        (entry) => (entry as File).name === fileItemToDelete.fileName
      );

      if (!matchedEntryToDelete) {
        alert(`Error: File "${fileItemToDelete.fileName}" is not found`);
        return prevFormData;
      }

      currFormDataFiles.delete(matchedEntryToDelete);

      const newFormData = new FormData();
      for (const file of currFormDataFiles) {
        newFormData.append(this.FORM_DATA_IDENTIFIER, file);
      }

      return newFormData;
    });
  }

  override uploadFile(fileItemToAdd: FilePreviewModel): Observable<UploadResponse> {
    const isFile = fileItemToAdd.file instanceof File;
    if (!isFile) {
      return this.throwUploadStatusError(fileItemToAdd, "Error: Upload is not a file.");
    }

    const hasProperExtension = this.allowedFileExtensions.some((ext) => fileItemToAdd.fileName.endsWith(ext));
    if (!hasProperExtension) {
      return this.throwUploadStatusError(
        fileItemToAdd,
        `Error: Upload has invalid file extension. Allowed file extensions: ${this.allowedFileExtensions.join(", ")}.`
      );
    }

    const currFormDataFiles = new Set(this.formData().getAll(this.FORM_DATA_IDENTIFIER));
    const isFileExistingAlready = [...currFormDataFiles].some(
      (entry) => (entry as File).name === fileItemToAdd.fileName
    );
    if (isFileExistingAlready) {
      return this.throwUploadStatusError(
        fileItemToAdd,
        `Error: File "${fileItemToAdd.fileName}" has been already uploaded. Will manually remove this duplicate file(s)! Please only submit the newest version of the file for upload.`
      );
    }

    this.formData.update((prevFormData) => {
      prevFormData.append(this.FORM_DATA_IDENTIFIER, fileItemToAdd.file);
      return prevFormData;
    });

    return of({
      body: `[SUCCESS] File "${fileItemToAdd.fileName}" has been successfully uploaded`,
      status: UploadStatus.UPLOADED,
      progress: 100
    });
  }

  override removeFile(removedFileItem: FilePreviewModel): Observable<RemoveFileStatus> {
    let isSuccessful = false;

    if (removedFileItem.uploadResponse == null) {
      // eslint-disable-next-line no-console
      console.error("Removing a file that is not uploaded yet.");
      return of({ success: isSuccessful });
    }

    this.deleteItemFromFormData(removedFileItem);

    isSuccessful = true;
    return of({ success: isSuccessful });
  }
}
