package main

import (
	"fmt"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
)

func main() {
	opts := protogen.Options{}
	opts.Run(func(plugin *protogen.Plugin) error {
		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}

			if err := proprdbgen.GenerateFile(plugin, file); err != nil {
				return fmt.Errorf("generate %s: %w", file.Desc.Path(), err)
			}
		}

		return nil
	})
}
