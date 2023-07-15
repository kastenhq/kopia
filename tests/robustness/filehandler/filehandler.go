//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package filehandler

import (
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// FileHandler implements a FileHandler struct.
type FileHandler struct {
}

// GetRootDir browses through the provided file path and return a path, ensuring that the first item is a file and not a folder. If the first item is a folder, it will continue to open directories until the condition of the first item being a file is met.
func (handler *FileHandler) GetRootDir(source string) string {
	path := source

	for {
		dirEntries, err := os.ReadDir(path)
		if err != nil {
			log.Println(err)
			return ""
		}

		if len(dirEntries) == 0 || !dirEntries[0].IsDir() {
			break
		}

		path = filepath.Join(path, dirEntries[0].Name())
	}
	return path
}

// ModifyDataSetWithContent appends the specified content to all files in the provided folder.
func (handler *FileHandler) ModifyDataSetWithContent(destination string, content string) error {
	dstDirs, err := os.ReadDir(destination)
	if err != nil {
		return err
	}

	for _, dstFile := range dstDirs {
		dstFilePath := filepath.Join(destination, dstFile.Name())

		dstFile, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}

		_, err = io.WriteString(dstFile, content)
		if err != nil {
			return err
		}

		dstFile.Close()
	}
	return nil
}

// CopyAllFiles implements copying all the files  from a source folder to a destination folder
func (handler *FileHandler) CopyAllFiles(source string, destination string) error {
	// Create the destination folder if it doesn't exist
	err := os.MkdirAll(destination, 0755)
	if err != nil {
		return err
	}

	srcDirs, err := os.ReadDir(source)
	if err != nil {
		return err
	}

	for _, file := range srcDirs {
		sourcePath := filepath.Join(source, file.Name())
		destinationPath := filepath.Join(destination, file.Name())

		// Open the source file
		sourceFile, err := os.Open(sourcePath)
		if err != nil {
			return err
		}

		destinationFile, err := os.Create(destinationPath)
		if err != nil {
			return err
		}

		_, err = io.Copy(destinationFile, sourceFile)
		if err != nil {
			return err
		}

		sourceFile.Close()
		destinationFile.Close()
	}
	return nil
}

// CompareDirs examines and compares the quantities and contents of files in two different folders
func (handler *FileHandler) CompareDirs(source string, destination string) error {
	srcDirs, err := os.ReadDir(source)
	if err != nil {
		return err
	}

	dstDirs, err := os.ReadDir(destination)
	if err != nil {
		return err
	}

	if len(dstDirs) != len(srcDirs) {
		return errors.New("The number of items in the two folders is not the same. " + source + " has " + strconv.Itoa(len(srcDirs)) + ". " + destination + " has " + strconv.Itoa(len(dstDirs)))
	}

	checkSet := make(map[string]bool)

	for _, dstDir := range dstDirs {
		checkSet[dstDir.Name()] = true
	}

	for _, srcDir := range srcDirs {
		_, ok := checkSet[srcDir.Name()]
		if !ok {
			return errors.New(srcDir.Name() + " is not in the folder:" + destination)
		}

		srcFilePath := filepath.Join(source, srcDir.Name())
		dstFilePath := filepath.Join(destination, srcDir.Name())

		cmd := exec.Command("cmp", "-s", srcFilePath, dstFilePath)
		err := cmd.Run()
		if err != nil {
			return errors.New("Files '" + srcFilePath + "' and '" + dstFilePath + "' are different.")
		}
	}
	return nil
}