package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
	"log"
	"reflect"
	"sort"
	"strings"
)

import "C"

type HCLObject struct {
	Type string
	Name string
}

const BlockPrefix = "@block:"
const ExprPrefix = "@expr:"
const RawStringPrefix = "@raw:"

func mapInterfaceInterfaceToStringInterface(m map[interface{}]interface{}) map[string]interface{} {
	resp := make(map[string]interface{})
	for key, val := range m {
		resp[key.(string)] = val
	}
	return resp
}

func handleStringPrefixes(key string, value interface{}, quote bool) (processedKey, line string) {
	switch {
	case strings.HasPrefix(key, RawStringPrefix):
		processedKey = strings.Replace(key, RawStringPrefix, "", 1)
		if quote {
			line = fmt.Sprintf("%q = %q", processedKey, value)
			return
		}
		line = fmt.Sprintf("%v = %q", processedKey, value)
		return
	case strings.HasPrefix(key, ExprPrefix):
		processedKey = strings.Replace(key, ExprPrefix, "", 1)
		if quote {
			line = fmt.Sprintf("%q = %v", processedKey, value)
			return
		}
		line = fmt.Sprintf("%v = %v", processedKey, value)
		return
	default:
		processedKey = key
		if quote {
			line = fmt.Sprintf("%q = \"%v\"", processedKey, value)
			return
		}
		line = fmt.Sprintf("%v = \"%v\"", processedKey, value)
		return
	}
}

func processString(resourceBody *hclwrite.Body, key string, value interface{}, debug bool) {
	var line string
	var processedKey string

	processedKey, line = handleStringPrefixes(key, value, false)

	if debug {
		log.Printf("Processing line %v\n", line)
	}
	parsedLine, diags := hclwrite.ParseConfig([]byte(line), "", hcl.Pos{Line: 1, Column: 1})
	if len(diags) != 0 {
		for _, diag := range diags {
			log.Printf("- %s", diag.Error())
		}
		log.Fatalf("unexpected diagnostics on line %v", line)
	}
	attr := parsedLine.Body().GetAttribute(processedKey)
	resourceBody.SetAttributeRaw(processedKey, attr.Expr().BuildTokens(nil))
}

func handleValuesInMap(value interface{}) (hclMapString string) {
	valMap := value.(map[string]interface{})
	var kvPairs []string
	nonStringMap := make(map[string]interface{})
	for k, v := range valMap {
		if reflect.TypeOf(v).Kind() == reflect.String {
			_, line := handleStringPrefixes(k, v, true)
			kvPairs = append(kvPairs, line)
		} else {
			_, line := handleStringPrefixes(ExprPrefix+k, v, true)
			kvPairs = append(kvPairs, line)
		}
		//kvPairs = append(kvPairs, fmt.Sprintf(`%v = %v`, k, v))
	}
	nonStringJson, _ := json.Marshal(nonStringMap)
	nonStringItems := string(nonStringJson)[1 : len(string(nonStringJson))-1]
	kvPairs = append(kvPairs, nonStringItems)
	finalMapValues := strings.Join(kvPairs[:len(kvPairs)-1], ", ")
	hclMapString = "{" + finalMapValues + "}"
	return
}

func processMap(resourceBody *hclwrite.Body, key string, value interface{}, debug bool) {

	line := fmt.Sprintf("%v = %v", key, handleValuesInMap(value))
	if debug {
		log.Printf("Processing line %v\n", line)
	}
	parsedLine, diags := hclwrite.ParseConfig([]byte(line), "", hcl.Pos{Line: 1, Column: 1})
	if len(diags) != 0 {
		for _, diag := range diags {
			log.Printf("- %s", diag.Error())
		}
		log.Fatalf("unexpected diagnostics on line %v", line)
	}
	attr := parsedLine.Body().GetAttribute(key)
	resourceBody.SetAttributeRaw(key, attr.Expr().BuildTokens(nil))
}

func processBlock(resourceBody *hclwrite.Body, key string, val interface{}, debug bool) {
	if strings.HasPrefix(key, BlockPrefix) {
		switch reflect.TypeOf(val).Kind() {
		case reflect.Slice:
			for _, v := range val.([]interface{}) {
				processBlock(resourceBody, key, v, debug)
			}
			return
		case reflect.Map:
			block := resourceBody.AppendNewBlock(strings.Replace(key, BlockPrefix, "", 1), nil)
			valCasted, ok := val.(map[string]interface{})
			if ok {
				processBody(block.Body(), valCasted, debug)
			}
			valInterfaceCasted, ok := val.(map[interface{}]interface{})
			if ok {
				processBody(block.Body(), mapInterfaceInterfaceToStringInterface(valInterfaceCasted), debug)
			}
		}
	}
}

func keyIsOfPrefix(key string, ty string) bool {
	return strings.HasPrefix(key, ty)
}

func keyIsNotOfPrefix(key string, ty string) bool {
	return !keyIsOfPrefix(key, ty)
}

func processBody(resourceBody *hclwrite.Body, input map[string]interface{}, debug bool) {
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := input[key]

		if keyIsOfPrefix(key, BlockPrefix) {
			processBlock(resourceBody, key, val, debug)
		}

		valType := reflect.TypeOf(val)
		if debug == true {
			log.Printf("%v is of type %v", key, valType)
			log.Printf("%v is of kind %v", key, valType.Kind())
			log.Printf("%v has value %v", key, val)
		}

		switch valType.Kind() {
		case reflect.Map:
			if keyIsNotOfPrefix(key, BlockPrefix) {
				processMap(resourceBody, key, val, debug)
			}
		case reflect.Bool:
			resourceBody.SetAttributeValue(key, cty.BoolVal(val.(bool))) // this is overwritten later
		case reflect.Float64:
			resourceBody.SetAttributeValue(key, cty.NumberFloatVal(val.(float64))) // this is overwritten later
		case reflect.String:
			processString(resourceBody, key, val, debug)

		}
	}
}

func WriteHCLFromMap(objectType string, input map[string]interface{}, resourceInfo HCLObject, debug bool) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	var resourceBlock *hclwrite.Block
	if resourceInfo.Type == "" {
		resourceBlock = rootBody.AppendNewBlock(objectType, []string{resourceInfo.Name})
	} else {
		resourceBlock = rootBody.AppendNewBlock(objectType, []string{resourceInfo.Type, resourceInfo.Name})
	}
	resourceBody := resourceBlock.Body()

	if input == nil {
		return ""
	}
	processBody(resourceBody, input, debug)

	return strings.Replace(string(f.Bytes()), "$${", "${", -1)
}

//export CreateHCLFromJson
func CreateHCLFromJson(objectType, objectName, objectIdentifier, jsonData string, debug bool) (*C.char, *C.char) {

	r := HCLObject{
		Type: objectName,
		Name: objectIdentifier,
	}
	var unJson map[string]interface{}
	err := json.Unmarshal([]byte(jsonData), &unJson)
	if err != nil {
		return C.CString(""), C.CString(err.Error())
	}
	if debug {
		return C.CString(WriteHCLFromMap(objectType, unJson, r, true)), C.CString("")
	}
	return C.CString(WriteHCLFromMap(objectType, unJson, r, false)), C.CString("")
}

//export ValidateHCL
func ValidateHCL(hclB64String string, debug bool) *C.char {
	var errorLines []string
	if debug {
		log.Printf("Validating HCL string: \n\n%s\n", hclB64String)
	}
	_, diags := hclwrite.ParseConfig([]byte(hclB64String), "", hcl.Pos{Line: 1, Column: 1})
	if len(diags) != 0 {
		for _, diag := range diags {
			errorLines = append(errorLines, fmt.Sprintf("- %s", diag.Error()))
		}
		return C.CString(strings.Join(errorLines, "\n"))
	}
	return C.CString("")
}

func main() {}
