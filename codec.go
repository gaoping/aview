package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	s "strings"

	"github.com/gocql/gocql"
	mapConvt "github.com/mitchellh/mapstructure"
	"github.com/ugorji/go/codec"
)

const (
	cborTagBase int = iota
	cborTagId
	cborTagTransactionId
	cborTagKind
	cborTagVersion
	cborTagAction
	cborTagFields
	cborTagStatus
	cborTagError
	cborTagCreatedOn
)

const (
	CBOR_INDEX = "cbor_index"

	ID             = "id"
	TRANSACTIOINID = "transactionid"
	KIND           = "kind"
	ERROR          = "error"
	ACTION         = "action"
	STATUS         = "status"
	FIELDS         = "fields"
	VERSION        = "version"
	CREATEDON      = "createdon"
)

const (
	// indexing to reduce size further
	error_map   = "out_of_memory,100,out_of_range,101"
	kind_map    = "pg.element,1,pg.event,2,pg.event.diagnostic,3,pg.event.revoke,0"
	action_map  = "activate,1,suspend,2,resume,3,deactivate,4"
	status_map  = "unknown,0,trying,100,ok,200,bad_request,400,internal_server_error,500,not_implemented,501"
	jsontag_map = "id,1,transactionid,2,kind,3,fields,6,version,4,action,5,status,7,error,8,createdon,9"
)

type Message struct {
	Id            gocql.UUID             `json:"id,omitempty" cbor_index:"1"`
	TransactionId gocql.UUID             `json:"transactionid,omitempty" cbor_index:"2"`
	Kind          string                 `json:"kind,omitempty" cbor_index:"3" `
	Version       string                 `json:"version,omitempty" cbor_index:"4"`
	Action        string                 `json:"action,omitempty" cbor_index:"5" `
	Status        string                 `json:"status,omitempty" cbor_index:"7" `
	Error         string                 `json:"error,omitempty" cbor_index:"8"`
	CreatedOn     string                 `json:"createdon,omitempty" cbor_index:"9"`
	Fields        map[string]interface{} `json:"fields,omitempty" cbor_index:"6"`
}

type CborMessage struct {
	CborData []byte
}

var idUuid, _ = gocql.ParseUUID("820e4b14-e857-4664-b6a0-9f119d58c3bb")
var tidUuid, _ = gocql.ParseUUID("750fd9fe-e2ec-4f76-be68-693b3baef629")

var msgme = Message{
	Id:            idUuid,
	TransactionId: tidUuid,
	Kind:          "pg.element",
	Version:       "1.0",
	Status:        "ok",
	Error:         "out_of_memory",
	Action:        "activate",
	CreatedOn:     "2029-02-01 16:16:16",
	Fields:        map[string]interface{}{"location": "lexingtonMA", "level": 3, "some": 2.9},
}

func main() {

	fmt.Printf("\n\tCheck message: %+v\n", msgme)
	cborData, err := msgme.Encode(true)
	if err != nil {
		fmt.Println("Error in encoding json")
	}
	fmt.Printf("\n\tCborData Size: %d: \n", binary.Size(cborData))
	fmt.Printf("\n\tCborData in Hex: \t% X \n", cborData)
}

// marshal json bytes to cbor bytes
func MarshalCbor(data []byte, indexed bool) ([]byte, error) {
	message := &Message{}

	jsonErr := json.Unmarshal(data, message)
	if jsonErr != nil {
		return nil, jsonErr
	}

	return message.Encode(indexed)
}

// encode message object to cbor bytes
func (message *Message) Encode(indexed bool) ([]byte, error) {
	var cborData []byte = make([]byte, 0, 64)
	handle := new(codec.CborHandle)
	var enc *codec.Encoder = codec.NewEncoderBytes(&cborData, handle)

	if indexed {
		cborMap := message.reflect()
		cborMap["id"] = "820e4b14-e857-4664-b6a0-9f119d58c3bb"
		fmt.Printf("\n\tID: %s", "820e4b14-e857-4664-b6a0-9f119d58c3bb")
		err := enc.Encode(cborMap)
		if err != nil {

			fmt.Printf("Error in encoding cbor: %+v", err)
			return nil, err
		}
	} else {
		err := enc.Encode(message)
		if err != nil {

			fmt.Printf("Error in encoding cbor message: %+v", err)
			return nil, err
		}
	}

	return cborData, nil
}

// reflect to index tags and values
func (message *Message) reflect() map[interface{}]interface{} {
	values := make(map[interface{}]interface{})
	val := reflect.ValueOf(message).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		cborTagIndex, err := strconv.ParseInt(tag.Get(CBOR_INDEX), 10, 64)
		if err != nil {
			fmt.Printf("\n\tnot an integer")
		}

		switch int(cborTagIndex) {
		case cborTagKind:
			var tag interface{}
			_, er := ToIndex(jsontag_map, KIND)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = KIND
			}
			valueFieldIndex, err := ToIndex(kind_map, valueField.Interface().(string))
			if err == nil {
				values[tag] = valueFieldIndex
			} else if valueField.Interface().(string) != "" {
				values[tag] = valueField.Interface().(string)
			}

		case cborTagAction:
			var tag interface{}
			_, er := ToIndex(jsontag_map, ACTION)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = ACTION
			}
			valueFieldIndex, err := ToIndex(action_map, valueField.Interface().(string))
			if err == nil {
				values[tag] = valueFieldIndex
			} else if valueField.Interface().(string) != "" {
				values[tag] = valueField.Interface().(string)
			}
		case cborTagStatus:
			var tag interface{}
			_, er := ToIndex(jsontag_map, STATUS)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = STATUS
			}
			valueFieldIndex, err := ToIndex(status_map, valueField.Interface().(string))
			if err == nil {
				values[tag] = valueFieldIndex
			} else if valueField.Interface().(string) != "" {
				values[tag] = valueField.Interface().(string)
			}
		case cborTagError:
			var tag interface{}
			_, er := ToIndex(jsontag_map, ERROR)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = ERROR
			}
			valueFieldIndex, err := ToIndex(error_map, valueField.Interface().(string))
			if err == nil {
				values[tag] = valueFieldIndex
			} else if valueField.Interface().(string) != "" {
				values[tag] = valueField.Interface().(string)
			}
		case cborTagId:
			var tag interface{}
			_, er := ToIndex(jsontag_map, ID)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = ID
			}
			values[tag] = message.Id.Bytes()
		case cborTagCreatedOn:
			var tag interface{}
			_, er := ToIndex(jsontag_map, CREATEDON)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = CREATEDON
			}
			values[tag] = message.CreatedOn
		case cborTagVersion:
			var tag interface{}
			_, er := ToIndex(jsontag_map, VERSION)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = VERSION
			}
			values[tag] = message.Version
		case cborTagFields:
			var tag interface{}
			_, er := ToIndex(jsontag_map, FIELDS)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = FIELDS
			}
			for k, v := range message.Fields {
				switch v.(type) {
				case json.Number, int, float64, float32:
					intVal, _ := getInt(v)
					message.Fields[k] = intVal
				case map[string]interface{}:
					fldMap := v.(map[string]interface{})
					for key, val := range v.(map[string]interface{}) {
						switch val.(type) {
						case json.Number, int, float64, float32:
							intVal, _ := getInt(val)
							fldMap[key] = intVal
						}
					}
				}
			}
			values[tag] = message.Fields
		case cborTagTransactionId:
			var tag interface{}
			_, er := ToIndex(jsontag_map, TRANSACTIOINID)
			if er == nil {
				tag = int(cborTagIndex)
			} else {
				tag = TRANSACTIOINID
			}
			values[tag] = message.TransactionId.Bytes()
		default:

		}
	}

	return values
}

// decode cbor bytes to interface
func (cm *CborMessage) Decode(indexed bool) (interface{}, error) {
	msg := Message{}

	var m interface{}
	err := codec.NewDecoderBytes(cm.CborData, new(codec.CborHandle)).Decode(&m)
	if err != nil {
		return nil, err
	}

	if indexed != true {
		switch m.(type) {
		case map[interface{}]interface{}:
			for key, val := range m.(map[interface{}]interface{}) {
				key64, isInt := key.(uint64)
				keyString, isString := key.(string)
				keyInt := int(key64)
				if (isInt && keyInt == cborTagId) || (isString && keyString == ID) {
					valBytes, isValBytes := val.([]byte)
					valString, isValString := val.(string)
					if isValBytes {
						id, err := gocql.UUIDFromBytes(valBytes)
						if err == nil {
							msg.Id = id
						}
					} else if isValString {
						id, err := gocql.ParseUUID(valString)
						if err == nil {
							msg.Id = id
						}
					}
				} else if (isInt && keyInt == cborTagTransactionId) || (isString && keyString == TRANSACTIOINID) {
					valBytes, isValBytes := val.([]byte)
					valString, isValString := val.(string)
					if isValBytes {
						id, err := gocql.UUIDFromBytes(valBytes)
						if err == nil {
							msg.TransactionId = id
						}
					} else if isValString {
						tid, err := gocql.ParseUUID(valString)
						if err == nil {
							msg.TransactionId = tid
						}
					}
				}
			}
		}
		err := mapConvt.Decode(m, &msg)
		if err != nil && s.Contains(err.Error(), "unsupported type: array") == false {
			fmt.Printf("\n Unable to decode map")
			return nil, err
		}

		return msg, nil
	}

	switch dataType := m.(type) {
	case map[interface{}]interface{}:
		for key, val := range m.(map[interface{}]interface{}) {

			key64, isInt := key.(uint64)
			keyString, isString := key.(string)

			keyInt := int(key64)
			if (isInt && keyInt == cborTagKind) || (isString && keyString == KIND) {
				valInt, isValInt := val.(uint64)
				_, isValString := val.(string)
				if isValInt {
					v := strconv.Itoa(int(valInt))
					msg.Kind = ToValue(kind_map, v)
				} else if isValString {
					msg.Kind = val.(string)
				}
			} else if isInt && keyInt == cborTagAction || isString && keyString == ACTION {
				valInt, isValInt := val.(uint64)
				_, isValString := val.(string)
				if isValInt {
					v := strconv.Itoa(int(valInt))
					msg.Action = ToValue(action_map, v)
				} else if isValString {
					msg.Action = val.(string)
				}
			} else if isInt && keyInt == cborTagStatus || isString && keyString == STATUS {
				valInt, isValInt := val.(uint64)
				_, isValString := val.(string)
				if isValInt {
					v := strconv.Itoa(int(valInt))
					msg.Status = ToValue(status_map, v)
				} else if isValString {
					msg.Status = val.(string)
				}
			} else if isInt && keyInt == cborTagError || isString && keyString == ERROR {
				valInt, isValInt := val.(uint64)
				_, isValString := val.(string)
				if isValInt {
					v := strconv.Itoa(int(valInt))
					msg.Error = ToValue(error_map, v)
				} else if isValString {
					msg.Error = val.(string)
				}
			} else if isInt && keyInt == cborTagId || isString && keyString == ID {
				valBytes, isValBytes := val.([]byte)
				valString, isValString := val.(string)
				if isValBytes {
					id, err := gocql.UUIDFromBytes(valBytes)
					if err == nil {
						msg.Id = id
					}
				} else if isValString {
					id, err := gocql.ParseUUID(valString)
					if err == nil {
						msg.Id = id
					}
				}
			} else if isInt && keyInt == cborTagTransactionId || isString && keyString == TRANSACTIOINID {
				valBytes, isValBytes := val.([]byte)
				valString, isValString := val.(string)
				if isValBytes {
					id, err := gocql.UUIDFromBytes(valBytes)
					if err == nil {
						msg.TransactionId = id
					}
				} else if isValString {
					tid, err := gocql.ParseUUID(valString)
					if err == nil {
						msg.TransactionId = tid
					}
				}
			} else if isInt && keyInt == cborTagVersion || isString && keyString == VERSION {
				msg.Version = val.(string)
			} else if isInt && keyInt == cborTagCreatedOn || isString && keyString == CREATEDON {
				msg.CreatedOn = val.(string)
			} else if isInt && keyInt == cborTagFields || isString && keyString == FIELDS {
				switch v := val.(type) {
				case map[interface{}]interface{}:
					fields := make(map[string]interface{}, 0)
					for fieldKey, fieldVal := range val.(map[interface{}]interface{}) {
						switch fieldKey.(type) {
						case string:
							mapString := make(map[string]interface{})
							switch fieldVal.(type) {
							case map[interface{}]interface{}:
								for key, val := range fieldVal.(map[interface{}]interface{}) {
									strKey := fmt.Sprintf("%s", key)
									innerMap := make(map[string]interface{})
									switch val.(type) {
									case map[interface{}]interface{}:
										for k, v := range val.(map[interface{}]interface{}) {
											innerStrkey := fmt.Sprintf("%s", k)
											innerMap[innerStrkey] = v
										}
										mapString[strKey] = innerMap
									default:
										mapString[strKey] = val
									}
								}
								fields[fieldKey.(string)] = mapString
							case []interface{}:
								logArr := make([]map[string]interface{}, 0)
								for i, entry := range fieldVal.([]interface{}) {
									switch entry.(type) {
									case map[interface{}]interface{}:
										for k, v := range entry.(map[interface{}]interface{}) {
											strkey := fmt.Sprintf("%s", k)
											mapString[strkey] = v
										}
									}
									logArr[i] = mapString
								}
								fields[fieldKey.(string)] = logArr
							default:
								fields[fieldKey.(string)] = fieldVal
							}
						}
					}
					msg.Fields = fields
				default:
					fmt.Printf("Unknown fields data type: %+v", v)
				}
			}

		}
	default:
		fmt.Printf("Unknown data type: %+v", dataType)
	}

	return msg, nil
}

func (cm *CborMessage) SimpleDecode() (interface{}, error) {
	var m interface{}
	err := codec.NewDecoderBytes(cm.CborData, new(codec.CborHandle)).Decode(&m)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Simply decoded cbor: %+v", m)
	return m, nil
}

// utils to get index of the value
func ToIndex(content string, value string) (int, error) {
	startIndex := s.Index(","+content, ","+value+",")
	if startIndex < 0 {
		return 0, errors.New("not found")
	}

	startIndex += len(value) + 1
	endIndex := s.Index(content[startIndex:], ",")
	if endIndex <= 0 {
		return 0, errors.New("not found")
	}

	endIndex += startIndex

	indexValue := content[startIndex:endIndex]
	index, err := strconv.ParseInt(indexValue, 10, 64)
	if err != nil {
		return 0, err
	}
	return int(index), nil
}

// utils to get value of the index
func ToValue(content string, value string) string {
	index := s.Index(content, ","+value)
	if index <= 0 {
		return ""
	}

	endIndex := s.LastIndex(content[0:index], ",")
	endIndex = endIndex + 1

	if endIndex < 0 {
		return ""
	}

	return content[endIndex:index]
}

func getInt(unk interface{}) (int, error) {
	if v_flt, ok := unk.(float64); ok {
		return int(v_flt), nil
	} else if v_int, ok := unk.(int); ok {
		return v_int, nil
	} else if v_int, ok := unk.(int16); ok {
		return int(v_int), nil
	} else if v_flt, ok := unk.(float32); ok {
		return int(v_flt), nil
	} else if v_jn, ok := unk.(json.Number); ok {
		v, _ := v_jn.Int64()
		return int(v), nil
	}
	return 0, errors.New("unknow type of number")
}
