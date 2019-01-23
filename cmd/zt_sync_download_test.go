package cmd

import (
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultLogVerbosityForSync = "WARNING"
	defaultOutputFormatForSync = "text"
)

func runSyncAndVerify(c *chk.C, raw rawSyncCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func getDefaultRawInput(src, dst string) rawSyncCmdArgs {
	return rawSyncCmdArgs{
		src:          src,
		dst:          dst,
		recursive:    true,
		logVerbosity: defaultLogVerbosityForSync,
		output:       defaultOutputFormatForSync,
		force:        false,
	}
}

func (s *cmdIntegrationSuite) TestSyncDownloadWithEmptyDestination(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with an empty folder
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		lookupMap := scenarioHelper{}.convertListToMap(blobList)
		for _, transfer := range mockedRPC.transfers {
			localRelativeFilePath := strings.Replace(transfer.Destination, dstDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)

			// look up the source blob, make sure it matches
			_, blobExist := lookupMap[localRelativeFilePath]
			c.Assert(blobExist, chk.Equals, true)
		}
	})

	// turn off recursive, this time only top blobs should be transferred
	raw.recursive = false
	mockedRPC.reset()

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)
		c.Assert(len(mockedRPC.transfers), chk.Not(chk.Equals), len(blobList))

		for _, transfer := range mockedRPC.transfers {
			localRelativeFilePath := strings.Replace(transfer.Destination, dstDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)
			c.Assert(strings.Contains(localRelativeFilePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
		}
	})
}

func (s *cmdIntegrationSuite) TestSyncDownloadWithIdenticalDestination(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateFilesFromList(c, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// wait for 1 second so that the last modified times of the blobs are guaranteed to be newer
	time.Sleep(time.Second)

	// refresh the blobs' last modified time so that they are newer
	scenarioHelper{}.generateBlobs(c, containerURL, blobList)
	mockedRPC.reset()

	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobList))

		// validate that the right transfers were sent
		lookupMap := scenarioHelper{}.convertListToMap(blobList)
		for _, transfer := range mockedRPC.transfers {
			localRelativeFilePath := strings.Replace(transfer.Destination, dstDirName+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1)

			// look up the source blob, make sure it matches
			_, blobExist := lookupMap[localRelativeFilePath]
			c.Assert(blobExist, chk.Equals, true)
		}
	})
}

// validate the bug fix for this scenario
func (s *cmdIntegrationSuite) TestSyncDownloadWithMissingDestination(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination as a missing folder
	dstDirName := filepath.Join(scenarioHelper{}.generateLocalDirectory(c), "imbatman")

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawInput(rawContainerURLWithSAS.String(), dstDirName)

	runSyncAndVerify(c, raw, func(err error) {
		// error should not be nil, but the app should not crash either
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

func (s *cmdIntegrationSuite) TestSyncDownloadWithSingleFile(c *chk.C) {
	bsu := getBSU()

	// set up the container with a single blob
	blobName := "singleblobisbest"
	blobList := []string{blobName}
	containerURL, containerName := createNewContainer(c, bsu)
	scenarioHelper{}.generateBlobs(c, containerURL, blobList)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	// set up the destination as a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	dstFileName := blobName
	scenarioHelper{}.generateFilesFromList(c, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
	raw := getDefaultRawInput(rawBlobURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

	// the file was created after the blob, so no sync should happen
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})

	// sleep for 1 sec so that the blob's last modified times are guaranteed to be newer
	time.Sleep(time.Second)

	// recreate the blob to have a later last modified time
	scenarioHelper{}.generateBlobs(c, containerURL, blobList)
	mockedRPC.reset()

	// the file was created after the blob, so no sync should happen
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, filepath.Join(dstDirName, dstFileName))
	})

}

// there is a type mismatch between the source and destination
func (s *cmdIntegrationSuite) TestSyncDownloadWithContainerAndFile(c *chk.C) {
	bsu := getBSU()

	// set up the container with numerous blobs
	containerURL, containerName := createNewContainer(c, bsu)
	blobList := scenarioHelper{}.generateCommonRemoteScenario(c, containerURL)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)
	c.Assert(len(blobList), chk.Not(chk.Equals), 0)

	// set up the destination as a single file
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	dstFileName := blobList[0]
	scenarioHelper{}.generateFilesFromList(c, dstDirName, blobList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawInput(rawContainerURLWithSAS.String(), filepath.Join(dstDirName, dstFileName))

	// type mismatch, we should get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}

// there is a type mismatch between the source and destination
func (s *cmdIntegrationSuite) TestSyncDownloadWithBlobAndDirectory(c *chk.C) {
	bsu := getBSU()

	// set up the container with a single blob
	blobName := "singleblobisbest"
	blobList := []string{blobName}
	containerURL, containerName := createNewContainer(c, bsu)
	scenarioHelper{}.generateBlobs(c, containerURL, blobList)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	// set up the destination as a directory
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
	raw := getDefaultRawInput(rawBlobURLWithSAS.String(), dstDirName)

	// type mismatch, we should get an error
	runSyncAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}
