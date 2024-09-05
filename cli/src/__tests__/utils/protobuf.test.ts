import { serializeProtobufToBuffer, deserializeBufferToProtobuf } from '../../utils/protobuf'
import { expect, describe, it } from '@jest/globals'
import logWrapper from '../../utils/logWrapper'
import * as protobuf from 'protobufjs'
import * as path from 'path'
import { jest } from '@jest/globals'

jest.mock('../../utils/logWrapper', () => ({
  fail: jest.fn(),
}))

const mockExit = jest.spyOn(process, 'exit').mockImplementation((code?: number) => {
  throw new Error(`Process exited with code ${code}`)
})

const mockProtoPath = path.join(__dirname, 'mockData/mockProto.proto')
const protoSchema = `
  syntax = "proto3";
  message SensorData {
    string deviceId = 1;
    string sensorType = 2;
    double value = 3;
    int64 timestamp = 4;
  }
`

// unified input & target output
const jsonMessage = '{"deviceId":"123456", "sensorType": "Temperature", "value": 22.5, "timestamp": 16700}'
const message = JSON.parse(jsonMessage)

const root = protobuf.parse(protoSchema).root
const SensorData = root.lookupType('SensorData')
const serializedMessage = SensorData.encode(SensorData.create(message)).finish()

// for serialization
const targetBuffer = Buffer.from(serializedMessage)

// for deserialization
const inputBuffer = Buffer.from(serializedMessage)
const targetMessage = JSON.stringify(SensorData.decode(inputBuffer).toJSON())

describe('protobuf', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('serializeProtobufToBuffer', () => {
    it('should serialize JSON message with a protobuf schema correctly', () => {
      const resultBuffer = serializeProtobufToBuffer(jsonMessage, mockProtoPath, 'SensorData')
      expect(resultBuffer).toBeInstanceOf(Buffer)

      expect(targetBuffer.equals(resultBuffer)).toBe(true)
    })

    it('should throw an error if input message does not follow JSON format', () => {
      const invalidInput = 'Not a JSON'

      expect(() => serializeProtobufToBuffer(invalidInput, mockProtoPath, 'SensorData')).toThrow()
      expect(logWrapper.fail).toHaveBeenCalledWith(expect.stringMatching(/Message serialization error:*/))
      expect(mockExit).toHaveBeenCalledWith(1)
    })

    it('should log an error and exit if message does not match schema', () => {
      const invalidMessage = Buffer.from('{"invalidField": "value"}')

      // BUG: Can not trigger line 17-18 of protobuf.ts
      // `protobuf.Type.verify` seems to be working in an unexpected way.
      // With `invalidMessage` as `raw`, no error is triggerred.
      expect(() => {
        serializeProtobufToBuffer(invalidMessage, mockProtoPath, 'SensorData')
      }).toThrow()
      expect(logWrapper.fail).toHaveBeenCalledWith(
        expect.stringMatching(/Unable to serialize message to protobuf buffer:*/),
      )
      expect(mockExit).toHaveBeenCalledWith(1)
    })

    it('should handle Buffer input correctly', () => {
      const bufferInput = Buffer.from(jsonMessage)

      const resultBuffer = serializeProtobufToBuffer(bufferInput, mockProtoPath, 'SensorData')

      expect(resultBuffer).toBeInstanceOf(Buffer)
      expect(targetBuffer.equals(resultBuffer)).toBe(true)
    })

    it('should throw an error if protobuf schema file is not found', () => {
      const nonExistentPath = 'non/existent/path.proto'
      expect(() => serializeProtobufToBuffer(jsonMessage, nonExistentPath, 'SensorData')).toThrow()
      expect(logWrapper.fail).toHaveBeenCalledWith(expect.stringMatching(/Message serialization error:*/))
      expect(mockExit).toHaveBeenCalledWith(1)
    })

    // INFO: only this test case can trigger line 17-18
    it('should handle verification errors from protobuf', () => {
      const invalidMessage = '{"deviceId": 123}' // deviceId should be a string
      expect(() => serializeProtobufToBuffer(invalidMessage, mockProtoPath, 'SensorData')).toThrow()
      expect(logWrapper.fail).toHaveBeenCalledWith(expect.stringMatching(/Message serialization error:*/))
      expect(mockExit).toHaveBeenCalledWith(1)
    })
  })

  describe('deserializeBufferToProtobuf', () => {
    it('should deserialize Buffer to string with a protobuf schema correctly', () => {
      // Test without format
      const resultWithoutFormat = deserializeBufferToProtobuf(inputBuffer, mockProtoPath, 'SensorData', false)
      expect(typeof resultWithoutFormat).toBe('string')

      expect(resultWithoutFormat as string).toEqual(targetMessage)
    })

    describe('should deserialize Buffer to string with format, Buffer without format', () => {
      it('should return JSON string without format', () => {
        const result = deserializeBufferToProtobuf(inputBuffer, mockProtoPath, 'SensorData', false)
        expect(typeof result).toBe('string')
        expect(() => JSON.parse(result as string)).not.toThrow()

        expect(result as string).toEqual(targetMessage)
      })

      it('should return Buffer with format', () => {
        const result = deserializeBufferToProtobuf(inputBuffer, mockProtoPath, 'SensorData', true)
        expect(result).toBeInstanceOf(Buffer)

        expect(result.toString()).toEqual(targetMessage)
      })
    })

    it('should log an error and exit if buffer is not valid protobuf', () => {
      const invalidBuffer = Buffer.from([0x08, 0x96, 0x01]) // An invalid protobuf buffer

      // BUG: Can not trigger line 42-43 of protobuf.ts
      // `protobuf.Type.verify` seems to be working in an unexpected way.
      // With `invalidBuffer` as `payload`, the decoded message is '{"deviceId":""}' and no error is triggerred.
      expect(() => deserializeBufferToProtobuf(invalidBuffer, mockProtoPath, 'SensorData', true)).toThrow()
      // error message is generated by function `transformPBJSError`
      expect(logWrapper.fail).toHaveBeenCalledWith(expect.stringMatching(/Message deserialization error:*/))
      expect(mockExit).toHaveBeenCalledWith(1)
    })

    it('should handle verification errors from protobuf during deserialization', () => {
      const unmatchedMessage = JSON.parse('{"invalidField": "value"}')
      const serializedMessage = SensorData.encode(SensorData.create(unmatchedMessage)).finish()

      const unmatchedBuffer = Buffer.from(serializedMessage)

      // BUG: Can not trigger line 42-43 of protobuf.ts
      // `protobuf.Type.verify` seems to be working in an unexpected way.
      // With `unmatchedBuffer` as `payload`, the decoded message is empty and no error is triggerred.
      expect(() => deserializeBufferToProtobuf(unmatchedBuffer, mockProtoPath, 'SensorData', true)).toThrow()
      expect(logWrapper.fail).toHaveBeenCalledWith(
        expect.stringMatching(/Unable to deserialize protobuf encoded buffer:*/),
      )
      expect(mockExit).toHaveBeenCalledWith(1)
    })
  })
})
