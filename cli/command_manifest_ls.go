package cli

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestListCommand = manifestCommands.Command("list", "List manifest items").Alias("ls").Default()
	manifestListFilter  = manifestListCommand.Flag("filter", "List of key:value pairs").Strings()
	manifestListSort    = manifestListCommand.Flag("sort", "List of keys to sort by").Strings()
)

func init() {
	manifestListCommand.Action(listManifestItems)
}

func listManifestItems(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	filter := map[string]string{}

	for _, kv := range *manifestListFilter {
		p := strings.Index(kv, ":")
		if p <= 0 {
			return fmt.Errorf("invalid list filter %q, missing ':'", kv)
		}

		filter[kv[0:p]] = kv[p+1:]
	}

	items := rep.Manifests.Find(filter)

	sort.Slice(items, func(i, j int) bool {
		for _, key := range *manifestListSort {
			if v1, v2 := items[i].Labels[key], items[j].Labels[key]; v1 != v2 {
				return v1 < v2
			}
		}

		return items[i].ModTime.Before(items[j].ModTime)
	})

	for _, it := range items {
		t := it.Labels["type"]
		fmt.Printf("%v %10v %v type:%v %v\n", it.ID, it.Length, it.ModTime.Local().Format(timeFormat), t, sortedMapValues(it.Labels))
	}

	return nil
}

func sortedMapValues(m map[string]string) string {
	var result []string

	for k, v := range m {
		if k == "type" {
			continue
		}
		result = append(result, fmt.Sprintf("%v:%v", k, v))
	}

	sort.Strings(result)
	return strings.Join(result, " ")
}
