package utils

import (
	"fmt"
	"os/exec"
)

func Chown(username string, path string, is_dir bool) error {
	// sudo chown -R username:username path
	var cmd *exec.Cmd
	if is_dir {
		cmd = exec.Command("/bin/sh", "-c", "sudo chown -R "+username+":"+username+" "+path)
	} else {
		cmd = exec.Command("/bin/sh", "-c", "sudo chown "+username+":"+username+" "+path)
	}

	return cmd.Run()
}

func RmFile(path string, is_dir bool) error {
	var cmd *exec.Cmd
	if is_dir {
		cmd = exec.Command("/bin/sh", "-c", "sudo rm -rf "+path)
	} else {
		cmd = exec.Command("/bin/sh", "-c", "sudo rm -f "+path)
	}

	return cmd.Run()
}

func ScpFile(username string, dest_path string, path string, dest string, is_dir bool) error {
	var cmd *exec.Cmd
	if is_dir {
		cmd = exec.Command("/bin/sh", "-c", "scp -r "+path+" "+username+"@"+dest+":"+dest_path)
	} else {
		cmd = exec.Command("/bin/sh", "-c", "scp "+path+" "+username+"@"+dest+":"+dest_path)
	}
	fmt.Println("scp -r " + path + " " + username + "@" + dest + ":" + dest_path)

	return cmd.Run()
}

func CpFile(src_path string, dest_path string, is_dir bool) error {
	var cmd *exec.Cmd
	if is_dir {
		cmd = exec.Command("/bin/sh", "-c", "sudo cp -r "+src_path+" "+dest_path)
	} else {
		cmd = exec.Command("/bin/sh", "-c", "sudo cp "+src_path+" "+dest_path)
	}

	return cmd.Run()
}

func MvFile(src_path string, dest_path string, is_dir bool) error {
	var cmd *exec.Cmd
	if is_dir {
		cmd = exec.Command("/bin/sh", "-c", "sudo mv -r "+src_path+" "+dest_path)
	} else {
		cmd = exec.Command("/bin/sh", "-c", "sudo mv "+src_path+" "+dest_path)
	}

	return cmd.Run()
}
