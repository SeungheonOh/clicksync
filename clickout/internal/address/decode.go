package address

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

var ErrInvalid = errors.New("invalid Cardano address")

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func Decode(value string) ([]byte, error) {
	if strings.HasPrefix(value, "hex:") {
		decoded, err := hex.DecodeString(strings.TrimPrefix(value, "hex:"))
		if err != nil || len(decoded) == 0 {
			return nil, ErrInvalid
		}
		return decoded, nil
	}
	if strings.HasPrefix(value, "addr1") || strings.HasPrefix(value, "addr_test1") {
		return decodeBech32(value)
	}
	return decodeBase58(value)
}

func decodeBech32(value string) ([]byte, error) {
	if value != strings.ToLower(value) && value != strings.ToUpper(value) {
		return nil, ErrInvalid
	}
	value = strings.ToLower(value)
	separator := strings.LastIndexByte(value, '1')
	if separator < 1 || separator+7 > len(value) {
		return nil, ErrInvalid
	}
	hrp := value[:separator]
	if hrp != "addr" && hrp != "addr_test" {
		return nil, ErrInvalid
	}
	data := make([]byte, 0, len(value)-separator-1)
	const charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
	for _, char := range value[separator+1:] {
		index := strings.IndexRune(charset, char)
		if index < 0 {
			return nil, ErrInvalid
		}
		data = append(data, byte(index))
	}
	if bech32Polymod(append(bech32HRPExpand(hrp), data...)) != 1 {
		return nil, ErrInvalid
	}
	payload, err := convertBits(data[:len(data)-6], 5, 8, false)
	if err != nil || len(payload) == 0 {
		return nil, ErrInvalid
	}
	return payload, nil
}

func bech32HRPExpand(hrp string) []byte {
	result := make([]byte, 0, len(hrp)*2+1)
	for _, char := range hrp {
		result = append(result, byte(char>>5))
	}
	result = append(result, 0)
	for _, char := range hrp {
		result = append(result, byte(char&31))
	}
	return result
}

func bech32Polymod(values []byte) uint32 {
	const (
		g0 uint32 = 0x3b6a57b2
		g1 uint32 = 0x26508e6d
		g2 uint32 = 0x1ea119fa
		g3 uint32 = 0x3d4233dd
		g4 uint32 = 0x2a1462b3
	)
	checksum := uint32(1)
	for _, value := range values {
		top := checksum >> 25
		checksum = (checksum&0x1ffffff)<<5 ^ uint32(value)
		if top&1 != 0 {
			checksum ^= g0
		}
		if top&2 != 0 {
			checksum ^= g1
		}
		if top&4 != 0 {
			checksum ^= g2
		}
		if top&8 != 0 {
			checksum ^= g3
		}
		if top&16 != 0 {
			checksum ^= g4
		}
	}
	return checksum
}

func convertBits(data []byte, fromBits, toBits uint, pad bool) ([]byte, error) {
	var accumulator uint32
	var bits uint
	maxValue := uint32(1<<toBits) - 1
	maxAccumulator := uint32(1<<(fromBits+toBits-1)) - 1
	result := make([]byte, 0, len(data)*int(fromBits)/int(toBits))
	for _, value := range data {
		if uint32(value)>>fromBits != 0 {
			return nil, ErrInvalid
		}
		accumulator = (accumulator<<fromBits | uint32(value)) & maxAccumulator
		bits += fromBits
		for bits >= toBits {
			bits -= toBits
			result = append(result, byte(accumulator>>bits&maxValue))
		}
	}
	if pad {
		if bits > 0 {
			result = append(result, byte(accumulator<<(toBits-bits)&maxValue))
		}
	} else if bits >= fromBits || byte(accumulator<<(toBits-bits))&byte(maxValue) != 0 {
		return nil, ErrInvalid
	}
	return result, nil
}

func decodeBase58(value string) ([]byte, error) {
	if value == "" {
		return nil, ErrInvalid
	}
	number := new(big.Int)
	base := big.NewInt(58)
	for _, char := range value {
		index := strings.IndexRune(base58Alphabet, char)
		if index < 0 {
			return nil, ErrInvalid
		}
		number.Mul(number, base)
		number.Add(number, big.NewInt(int64(index)))
	}
	decoded := number.Bytes()
	leading := 0
	for leading < len(value) && value[leading] == '1' {
		leading++
	}
	result := make([]byte, leading+len(decoded))
	copy(result[leading:], decoded)
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalid)
	}
	return result, nil
}
