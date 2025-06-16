package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/marcoalexf/golog/internal/log"
)

type Command struct {
	command string
	args    string
}

func main() {
	fmt.Println("GoLog node starting..")

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Enter commands, type 'exit' to quit:")

	log := log.NewLog()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if strings.ToUpper(input) == "EXIT" {
			fmt.Println("Exiting...")
			break
		}

		// Split into command and args on first space only
		var cmd Command
		parts := strings.SplitN(input, " ", 2)

		cmd.command = parts[0]
		if len(parts) > 1 {
			cmd.args = parts[1]
		} else {
			cmd.args = ""
		}

		output, err := CommandProcessor(cmd, log)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println(output)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

func CommandProcessor(command Command, log *log.Log) (string, error) {
	switch strings.ToUpper(command.command) {
	case "APPEND":
		offset, err := log.Append([]byte(command.args))
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(offset, 10), nil
	case "READ":
		offset, err := strconv.ParseUint(command.args, 10, 64)
		if err != nil {
			return "", err
		}
		record, err := log.Read(offset)
		if err != nil {
			return "", err
		}
		return string(record), nil
	default:
		return "", errors.New("Command invalid")
	}
}
