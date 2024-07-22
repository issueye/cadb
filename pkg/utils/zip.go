package utils

import (
	"archive/zip"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

//
// How to implement zip and unzip please refer to https://dablelv.blog.csdn.net/article/details/122441250.
//

// Zip compresses the specified files or dirs to zip archive.
// If a path is a dir don't need to specify the trailing path separator.
// For example calling Zip("archive.zip", "dir", "csv/baz.csv") will get archive.zip and the content of which is
// baz.csv
// dir
// ├── bar.txt
// └── foo.txt
// Note that if a file is a symbolic link on Linux it will be skipped.
// If you want to follow a symbolic link please use the function ZipFollowSymlink.
func Zip(zipPath string, paths ...string) error {
	// Create zip file and it's parent dir.
	if err := os.MkdirAll(filepath.Dir(zipPath), os.ModePerm); err != nil {
		return err
	}
	archive, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer archive.Close()

	// New zip writer.
	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	// Traverse the file or directory.
	for _, rootPath := range paths {
		// Remove the trailing path separator if path is a directory.
		rootPath = strings.TrimSuffix(rootPath, string(os.PathSeparator))

		// Visit all the files or directories in the tree.
		err = filepath.Walk(rootPath, walkFunc(rootPath, zipWriter))
		if err != nil {
			return err
		}
	}
	return nil
}

func walkFunc(rootPath string, zipWriter *zip.Writer) filepath.WalkFunc {
	return func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// If a file is a symbolic link it will be skipped.
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		// Create a local file header.
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// Set compression method.
		header.Method = zip.Deflate

		// Set relative path of a file as the header name.
		header.Name, err = filepath.Rel(filepath.Dir(rootPath), path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			header.Name += string(os.PathSeparator)
		}

		// Create writer for the file header and save content of the file.
		headerWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(headerWriter, f)
		return err
	}
}

// ZipFollowSymlink compresses the specified files or dirs to zip archive.
// If the specified files or dirs is a symbolic link ZipFollowSymlink will follow it.
// Note that the symbolic link need to avoid loops.
func ZipFollowSymlink(zipPath string, paths ...string) error {
	// Create zip file and it's parent dir.
	if err := os.MkdirAll(filepath.Dir(zipPath), os.ModePerm); err != nil {
		return err
	}
	archive, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer archive.Close()

	// New zip writer.
	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	// Get all the file or directory paths.
	var allFilePaths []string
	pathToRoot := make(map[string]string)
	for _, path := range paths {
		// If the path is a dir or symlink to dir, get all files in it.
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Remove the trailing path separator if path is a directory.
			path = strings.TrimSuffix(path, string(os.PathSeparator))
			filePaths, err := ListDirEntryPathsSymlink(path, true)
			if err != nil {
				return err
			}
			allFilePaths = append(allFilePaths, filePaths...)
			for _, p := range filePaths {
				pathToRoot[p] = path
			}
			continue
		}
		allFilePaths = append(allFilePaths, path)
		pathToRoot[path] = path
	}

	// Traverse all the file or directory.
	for _, path := range allFilePaths {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}

		// Create a local file header.
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// Set compression method.
		header.Method = zip.Deflate

		// Set relative path of a file as the header name.
		header.Name, err = filepath.Rel(filepath.Dir(pathToRoot[path]), path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			header.Name += string(os.PathSeparator)
		}

		// Create writer for the file header and save content of the file.
		headerWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		// If file is a directory.
		if info.IsDir() {
			continue
		}

		// If file is a file or symlink to file.
		realPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		f, err := os.Open(realPath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(headerWriter, f)
		if err != nil {
			return err
		}
	}
	return nil
}

// Unzip decompresses a zip file to specified directory.
// Note that the destination directory don't need to specify the trailing path separator.
// If the destination directory doesn't exist, it will be created automatically.
func Unzip(zipath, dir string) error {
	// Open zip file.
	reader, err := zip.OpenReader(zipath)
	if err != nil {
		return err
	}
	defer reader.Close()
	for _, file := range reader.File {
		if err := unzipFile(file, dir); err != nil {
			return err
		}
	}
	return nil
}

func unzipFile(file *zip.File, dir string) error {
	// Prevent path traversal vulnerability.
	// Such as if the file name is "../../../path/to/file.txt" which will be cleaned to "path/to/file.txt".
	name := strings.TrimPrefix(filepath.Join(string(filepath.Separator), file.Name), string(filepath.Separator))
	filePath := path.Join(dir, name)

	// Create the directory of file.
	if file.FileInfo().IsDir() {
		if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	// Open the file.
	r, err := file.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	// Create the file.
	w, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer w.Close()

	// Save the decompressed file content.
	_, err = io.Copy(w, r)
	return err
}

// ListDirEntryPathsSymlink lists all the file or dir paths in the directory recursively.
// If the cur is true result will include current directory.
func ListDirEntryPathsSymlink(dirname string, cur bool) ([]string, error) {
	// Remove the trailing path separator if dirname has.
	dirname = strings.TrimSuffix(dirname, string(os.PathSeparator))

	infos, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(infos))

	// Include current directory.
	if cur {
		paths = append(paths, dirname)
	}

	for _, info := range infos {
		path := dirname + string(os.PathSeparator) + info.Name()
		realInfo, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if realInfo.IsDir() {
			tmp, err := ListDirEntryPathsSymlink(path, cur)
			if err != nil {
				return nil, err
			}
			paths = append(paths, tmp...)
			continue
		}
		paths = append(paths, path)
	}
	return paths, nil
}
