package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"sync/atomic"

	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type syncDownloadEnumerator common.SyncJobPartOrderRequest

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncDownloadEnumerator) addTransferToDownload(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {
	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then set atomicSyncStatus
		// variable to 1
		if e.PartNumber == 0 {
			cca.setFirstPartOrdered()
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncDownloadEnumerator) dispatchFinalPart(cca *cookedSyncCmdArgs) error {
	numberOfCopyTransfers := len(e.CopyJobRequest.Transfers)
	numberOfDeleteTransfers := len(e.FilesToDeleteLocally)
	// if the number of transfer to copy / delete both are 0
	// and no part was dispatched, then it means there is no work to do
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		return errors.New("cannot start job because there are no transfer to upload or delete. " +
			"The source and destination are in sync")
	}

	if numberOfCopyTransfers > 0 {
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// If the JobPart sent was the first part, then set atomicSyncStatus to 1, so that progress reporting can start.
		if e.PartNumber == 0 {
			cca.setFirstPartOrdered()
		}
	}

	if numberOfDeleteTransfers > 0 {
		answer := ""
		if cca.force {
			answer = "y"
		} else {
			answer = glcm.Prompt(fmt.Sprintf("Sync has enumerated %v files to delete locally. Do you want to delete these files ? Please confirm with y/n: ", numberOfDeleteTransfers))
		}
		// read a line from stdin, if the answer is not yes, then is No, then ignore the transfers queued for deletion and continue
		if !strings.EqualFold(answer, "y") {
			if numberOfCopyTransfers == 0 {
				glcm.Exit("cannot start job because there are no transfer to upload or delete. "+
					"The source and destination are in sync", 0)
			}
			cca.isEnumerationComplete = true
			return nil
		}
		// TODO if the application crashes while deletions are happening, then this information is lost
		// TODO however, customers are less likely to resume a sync command. Need to confirm with Sercan.
		for _, file := range e.FilesToDeleteLocally {
			err := os.Remove(file)
			if err != nil {
				glcm.Info(fmt.Sprintf("error %s deleting the file %s", err.Error(), file))
			}
		}
		if numberOfCopyTransfers == 0 {
			glcm.Exit(fmt.Sprintf("sync completed. Deleted %v files locally ", len(e.FilesToDeleteLocally)), 0)
		}
	}
	cca.isEnumerationComplete = true
	return nil
}

// lists the blobs under the source and verifies whether the blob
// exists locally or not by checking the expected localPath of blob in the LocalFiles map. If the blob
// does exists, it compares the last modified time.
func (e *syncDownloadEnumerator) listSourceAndCompare(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// destRootPath is the path of destination without wildCards
	// For Example: cca.destination = C:\a1\a* , so destRootPath = C:\a1
	destRootPath, _ := util.sourceRootPathWithoutWildCards(cca.destination)

	// the source was already validated, it'd be surprising if we cannot parse it at this time
	sourceURL, err := url.Parse(cca.source)
	common.PanicIfErr(err)

	// since source is a remote url, it will have sas parameter
	// since sas parameter was stripped from the source url
	// while cooking the raw command arguments
	// source sas is re-added to url for listing the blobs.
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)

	// get the container URL
	blobUrlParts := azblob.NewBlobURLParts(*sourceURL)
	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}
	containerRawURL := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern, _ := blobURLPartsExtension.searchPrefixFromBlobURL()
	containerURL := azblob.NewContainerURL(containerRawURL, p)

	// virtual directory is the entire virtual directory path before the blob name
	// passed in the searchPrefix
	// Example: cca.destination = https://<container-name>/vd-1?<sig> searchPrefix = vd-1/
	// virtualDirectory = vd-1
	// Example: cca.destination = https://<container-name>/vd-1/vd-2/fi*.txt?<sig> searchPrefix = vd-1/vd-2/fi*.txt
	// virtualDirectory = vd-1/vd-2/
	virtualDirectory := util.getLastVirtualDirectoryFromPath(searchPrefix)
	// strip away the leading / in the closest virtual directory
	if len(virtualDirectory) > 0 && virtualDirectory[0:1] == "/" {
		virtualDirectory = virtualDirectory[1:]
	}

	// get the source path without the wildcards
	// this needs to be defined since the files mentioned with exclude&include flags are relative to the source
	parentSourcePath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentSourcePath = parentSourcePath[:pathSepIndex]
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// check if the listed blob segment does not matches the sourcePath pattern
			// if it does not comparison is not required
			if !util.matchBlobNameAgainstPattern(pattern, blobInfo.Name, cca.recursive) {
				continue
			}
			// relativePathOfBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.destination ="C:\User1\user-1" cca.source = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// relativePathOfBlobLocally = user-1/a.txt
			relativePathOfBlobLocally := util.relativePathToRoot(parentSourcePath, blobInfo.Name, '/')
			relativePathOfBlobLocally = strings.Replace(relativePathOfBlobLocally, virtualDirectory, "", 1)

			blobLocalPath := util.generateLocalPath(cca.destination, relativePathOfBlobLocally)

			// increment the number of files scanned at the source.
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)

			// calculate the expected local path of the blob
			blobLocalPath = util.generateLocalPath(destRootPath, relativePathOfBlobLocally)

			// if the files is found in the list of files to be excluded, then it is not compared
			_, found := e.SourceFilesToExclude[blobLocalPath]
			if found {
				continue
			}
			// Check if the blob exists in the map of local files. If the file is
			// found, compare the modified time of the file against the blob's last
			// modified time. If the modified time of file is later than the blob's
			// modified time, then queue transfer for upload. If not, then delete
			// blobLocalPath from the map of sourceFiles.
			localFileTime, found := e.LocalFiles[blobLocalPath]
			if found {
				if !blobInfo.Properties.LastModified.After(localFileTime) {
					delete(e.LocalFiles, blobLocalPath)
					continue
				}
			}
			err = e.addTransferToDownload(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(util.generateBlobUrl(containerRawURL, blobInfo.Name)).String(),
				Destination:      blobLocalPath,
				SourceSize:       *blobInfo.Properties.ContentLength,
				LastModifiedTime: blobInfo.Properties.LastModified,
			}, cca)

			if err != nil {
				return err
			}

			delete(e.LocalFiles, blobLocalPath)
		}
		marker = listBlob.NextMarker
	}
	return nil
}

func (e *syncDownloadEnumerator) listTheDestinationIfRequired(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (bool, error) {
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	util := copyHandlerUtil{}

	// get the files and directories for the given destination pattern
	listOfFilesAndDir, err := filepath.Glob(cca.destination)
	if err != nil {
		return false, fmt.Errorf("error finding the files and directories for destination pattern %s", cca.destination)
	}

	// isDstSingleFile is used to determine whether the given destination pattern represents single file or not
	// if the source is a single file, this pointer will not be nil
	// if it is nil, it means the source is a directory or list of files
	var isDstSingleFile os.FileInfo = nil

	if len(listOfFilesAndDir) == 0 {
		return false, fmt.Errorf("cannot scan the destination %s, please verify that it is a valid path", cca.destination)
	} else if len(listOfFilesAndDir) == 1 {
		fInfo, fError := os.Stat(listOfFilesAndDir[0])
		if fError != nil {
			return false, fmt.Errorf("cannot scan the destination %s, please verify that it is a valid path", listOfFilesAndDir[0])
		}
		if fInfo.Mode().IsRegular() {
			isDstSingleFile = fInfo
		}
	}

	// attempt to parse the source url
	sourceURL, err := url.Parse(cca.source)
	// the source should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// since source is a remote url, it will have sas parameter
	// sas parameter was stripped from the url while cooking the raw command arguments
	// source sas is re-added to url for listing the blobs
	sourceURL = util.appendQueryParamToUrl(sourceURL, cca.sourceSAS)
	blobUrl := azblob.NewBlobURL(*sourceURL, p)

	// get the blob Properties to see whether the source is a single blob
	bProperties, bPropertiesError := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	// sync only happens between the source and destination of same type
	// i.e between blob and local file or between virtual directory/container and local directory
	// if the source is a blob and destination is not a local file, sync fails.
	if isDstSingleFile != nil && bPropertiesError != nil {
		return true, fmt.Errorf("cannot perform sync between blob %s and non-file destination %s. Sync only happens between source and destination of same type", cca.source, cca.destination)
	}
	// if the source is a virtual directory/container and destination is a file
	if isDstSingleFile == nil && bPropertiesError == nil {
		return false, fmt.Errorf("cannot perform sync between virtual directory/container %s and file destination %s. Sync only happens between source and destination of same type", cca.source, cca.destination)
	}

	// if both source is a blob and destination is a file, then we need to do the comparison and queue the transfer if required
	if isDstSingleFile != nil && bPropertiesError == nil {
		blobName := sourceURL.Path[strings.LastIndex(sourceURL.Path, "/")+1:]
		// compare the blob name and file name, blobName and filename should be same for sync to happen
		if strings.Compare(blobName, isDstSingleFile.Name()) != 0 {
			return true, fmt.Errorf("sync cannot be done since blob %s and filename %s do not match", blobName, isDstSingleFile.Name())
		}

		// if the modified time of local file is later than that of blob
		// sync does not needs to happen
		if isDstSingleFile.ModTime().After(bProperties.LastModified()) {
			return true, fmt.Errorf("blob %s and file %s already in sync", blobName, isDstSingleFile.Name())
		}

		err = e.addTransferToDownload(common.CopyTransfer{
			Source:           util.stripSASFromBlobUrl(*sourceURL).String(),
			Destination:      listOfFilesAndDir[0],
			SourceSize:       bProperties.ContentLength(),
			LastModifiedTime: bProperties.LastModified(),
		}, cca)

		if err != nil {
			return true, err
		}

		return true, nil
	}

	// TODO it's pretty confusing how the patterns and include/exclude play together
	// TODO this command seems overly complicated and hard to use, we should reconsider the command syntax
	sourcePattern := ""
	// Parse the source URL into blob URL parts.
	blobUrlParts := azblob.NewBlobURLParts(*sourceURL)
	// get the root path without wildCards and get the source pattern
	// For Example: source = <container-name>/a*/*/*
	// rootPath = <container-name> sourcePattern = a*/*/*
	blobUrlParts.BlobName, sourcePattern = util.sourceRootPathWithoutWildCards(blobUrlParts.BlobName)

	getRelativePath := func(fileOrDir string) (localFileRelativePath string) {
		// replace the OS path separator in fileOrDir string with AZCOPY_PATH_SEPARATOR
		// this replacement is done to handle the windows file paths where path separator "\\"
		fileOrDir = strings.Replace(fileOrDir, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

		// localFileRelativePath is the path of file relative to root directory
		// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localFileRelativePath = \a.txt
		// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localFileRelativePath = \dir-2\a.txt
		localFileRelativePath = strings.Replace(fileOrDir, cca.destination, "", 1)
		// remove the path separator at the start of relative path
		if len(localFileRelativePath) > 0 && localFileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
			localFileRelativePath = localFileRelativePath[1:]
		}
		return
	}

	// iterate through each file/dir inside the destination
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err != nil {
			glcm.Info(fmt.Sprintf("cannot get the file info for %s. failed with error %s", fileOrDir, err.Error()))
		}
		// directories are uploaded only if recursive is on
		if f.IsDir() && cca.recursive {
			// walk goes through the entire directory tree
			err = filepath.Walk(fileOrDir, func(pathToFile string, fileInfo os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if fileInfo.IsDir() {
					return nil
				} else {
					localFileRelativePath := getRelativePath(pathToFile)

					// if the localFileRelativePath does not match the source pattern, then it is not compared
					if !util.matchBlobNameAgainstPattern(sourcePattern, localFileRelativePath, cca.recursive) {
						return nil
					}

					if util.resourceShouldBeExcluded(cca.destination, e.Exclude, pathToFile) {
						e.SourceFilesToExclude[pathToFile] = fileInfo.ModTime()
						return nil
					}
					if len(e.LocalFiles) > MaxNumberOfFilesAllowedInSync {
						glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), common.EExitCode.Error())
					}
					e.LocalFiles[pathToFile] = fileInfo.ModTime()
					// Increment the sync counter.
					atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
				}
				return nil
			})
		} else if !f.IsDir() {
			localFileRelativePath := getRelativePath(fileOrDir)

			// if the localFileRelativePath does not match the source pattern, then it is not compared
			if !util.matchBlobNameAgainstPattern(sourcePattern, localFileRelativePath, cca.recursive) {
				continue
			}

			if util.resourceShouldBeExcluded(cca.destination, e.Exclude, fileOrDir) {
				e.SourceFilesToExclude[fileOrDir] = f.ModTime()
				continue
			}

			if len(e.LocalFiles) > MaxNumberOfFilesAllowedInSync {
				glcm.Exit(fmt.Sprintf("cannot sync the source %s with more than %v number of files", cca.source, MaxNumberOfFilesAllowedInSync), common.EExitCode.Error())
			}
			e.LocalFiles[fileOrDir] = f.ModTime()
			// Increment the sync counter.
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}
	return false, nil
}

// queueLocalFilesForDeletion
func (e *syncDownloadEnumerator) queueLocalFilesForDeletion(cca *cookedSyncCmdArgs) {
	for file := range e.LocalFiles {
		e.FilesToDeleteLocally = append(e.FilesToDeleteLocally, file)
	}
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncDownloadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p, err := createBlobPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// the copy job request is used to send job part orders
	// the delete job request is not needed, since the delete operations are performed by the enumerator
	e.CopyJobRequest.JobID = e.JobID
	e.CopyJobRequest.FromTo = e.FromTo
	e.CopyJobRequest.SourceSAS = e.SourceSAS
	e.CopyJobRequest.DestinationSAS = e.DestinationSAS
	e.CopyJobRequest.BlobAttributes.PreserveLastModifiedTime = true
	e.CopyJobRequest.ForceWrite = true
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.CopyJobRequest.CommandString = e.CommandString
	e.CopyJobRequest.CredentialInfo = e.CredentialInfo
	e.LocalFiles = make(map[string]time.Time)
	e.SourceFilesToExclude = make(map[string]time.Time)

	// scanning progress should be printed
	cca.waitUntilJobCompletion(false)

	isSourceABlob, err := e.listTheDestinationIfRequired(cca, p)
	if err != nil {
		return err
	}

	// if the source provided is a blob, then remote doesn't needs to be compared against the local
	// since single blob already has been compared against the file
	if !isSourceABlob {
		err = e.listSourceAndCompare(cca, p)
		if err != nil {
			return err
		}
	}

	e.queueLocalFilesForDeletion(cca)

	// No Job Part has been dispatched, then dispatch the JobPart.
	if e.PartNumber == 0 ||
		len(e.CopyJobRequest.Transfers) > 0 ||
		len(e.DeleteJobRequest.Transfers) > 0 {
		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		cca.setFirstPartOrdered()
	}
	// scanning all the destination and is complete
	cca.setScanningComplete()
	return nil
}
