package main

import (
	"encoding/json"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
	"log"
	"reflect"
	"strings"
)

import "C"

type HCLObject struct {
	Type string
	Name string
}

func GetObjectType(inputMap map[string]interface{}) reflect.Type {
	for _, val := range inputMap {
		return reflect.TypeOf(val)
	}
	return nil
}

func mapInterfaceInterfaceToStringInterface(m map[interface{}]interface{}) map[string]interface{} {
	resp := make(map[string]interface{})
	for key, val := range m {
		resp[key.(string)] = val
	}
	return resp
}

func ProcessBody(resourceBody *hclwrite.Body, input map[string]interface{}) {
	prefix := "@block:"
	for key, val := range input {
		if strings.HasPrefix(key, prefix) {
			block := resourceBody.AppendNewBlock(strings.Replace(key, prefix,"", 1), nil)
			valCasted, ok := val.(map[string]interface{})
			if ok {
				ProcessBody(block.Body(), valCasted)
			}
			valInterfaceCasted, ok := val.(map[interface{}]interface{})
			if ok {
				ProcessBody(block.Body(), mapInterfaceInterfaceToStringInterface(valInterfaceCasted))
			}

		}
		valType := reflect.TypeOf(val)
		log.Printf("%v is of type %v", key, valType)
		log.Printf("%v is of kind %v", key, valType.Kind())
		switch valType.Kind() {
		case reflect.Map:
			if !strings.HasPrefix(key, prefix) {
				objectMap := make(map[string]cty.Value)
				valMap := val.(map[string]interface{})
				firstValType := GetObjectType(valMap)
				switch firstValType.Kind() {
				case reflect.Int:
					for key, val := range valMap {
						objectMap[key] = cty.NumberIntVal(int64(val.(int)))
					}
				case reflect.String:
					for key, val := range valMap {
						objectMap[key] = cty.StringVal(val.(string))
					}
				case reflect.Float64:
					for key, val := range valMap {
						objectMap[key] = cty.NumberFloatVal(val.(float64))
					}
				}
				resourceBody.SetAttributeValue(key, cty.ObjectVal(objectMap))
			}
		case reflect.Int:
			resourceBody.SetAttributeValue(key, cty.NumberIntVal(int64(val.(int)))) // this is overwritten later
		case reflect.Int64:
			resourceBody.SetAttributeValue(key, cty.NumberIntVal(val.(int64))) // this is overwritten later
		case reflect.Float64:
			resourceBody.SetAttributeValue(key, cty.NumberFloatVal(val.(float64))) // this is overwritten later
		case reflect.String:
			resourceBody.SetAttributeValue(key, cty.StringVal(val.(string))) // this is overwritten later
		}
	}
}

func WriteHCLFromMap(objectType string, input map[string]interface{}, resourceInfo HCLObject) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	resourceBlock := rootBody.AppendNewBlock(objectType, []string{resourceInfo.Type, resourceInfo.Name})
	resourceBody := resourceBlock.Body()

	if input == nil {
		return ""
	}
	ProcessBody(resourceBody, input)

	return strings.Replace(string(f.Bytes()), "$${", "${", 1)
}


//export CreateResourceHCL
func CreateResourceHCL(objectType, objectName, objectIdentifier, jsonData *C.char) (*C.char, *C.char) {

	r := HCLObject{
		Type: C.GoString(objectName),
		Name: C.GoString(objectIdentifier),
	}
	var unJson map[string]interface{}
	err := json.Unmarshal([]byte(C.GoString(jsonData)), &unJson)
	if err != nil {
		return C.CString(""), C.CString(err.Error())
	}
	return C.CString(WriteHCLFromMap(C.GoString(objectType), unJson, r)), C.CString("")
}

func main() {}