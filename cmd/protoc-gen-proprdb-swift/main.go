package main

import (
	"flag"
	"fmt"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
	pluginpb "google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	rawOptions := make(map[string]string)
	flags.Func("Visibility", "Swift access level for generated declarations", func(value string) error {
		rawOptions["Visibility"] = value
		return nil
	})
	flags.Func("PublicSynchronousAPI", "make the generated synchronous Swift API public", func(value string) error {
		rawOptions["PublicSynchronousAPI"] = value
		return nil
	})

	opts := protogen.Options{ParamFunc: flags.Set}
	opts.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures |= uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		options, err := proprdbgen.ParseSwiftGeneratorOptions(rawOptions)
		if err != nil {
			return err
		}

		files := make([]*protogen.File, 0)
		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}
			files = append(files, file)
		}
		if err := proprdbgen.GenerateSwiftFiles(plugin, files, options); err != nil {
			return fmt.Errorf("generate Swift module: %w", err)
		}

		return nil
	})
}
