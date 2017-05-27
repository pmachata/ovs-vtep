/*
 * ovs-vtep
 * Copyright (C) 2017 Petr Machata <pmachata@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

extern crate unix_socket;

extern crate serde;
#[macro_use] extern crate serde_json as sj;
#[macro_use] extern crate serde_derive;

use std::io::Write;
use std::str;
use std::fmt;
use std::collections::HashMap;
use serde::de::{SeqAccess, Visitor, Error};

#[derive(Serialize, Deserialize)]
struct JsonRpcError {
    error: String,
    details: String,
}

#[derive(Serialize, Deserialize)]
struct JsonRpcResult {
    id: u32,
    result: Option<sj::Value>,
    error: Option<JsonRpcError>,
}

struct JsonUuidVisitor;
struct JsonUuidSetVisitor;

impl JsonUuidVisitor {
    fn parse<'de, A>(val: sj::Value) -> Result<String, A::Error>
        where A: SeqAccess<'de>
    {
        if let sj::Value::Array(mut v) = val {
            if v.len() != 2 {
                return Err(A::Error::invalid_length(0, &"2"));
            }

            let emt2 = v.pop().unwrap();
            let emt1 = v.pop().unwrap();
            if let sj::Value::String(head) = emt1 {
                if head == "uuid" {
                    if let sj::Value::String(uuid) = emt2 {
                        return Ok(uuid);
                    }
                }
            }
        }

        return Err(A::Error::custom(&"expected [\"uuid\", <uuid>]"));
    }
}

impl<'de> Visitor<'de> for JsonUuidVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("UUID")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where A: SeqAccess<'de>
    {
        let mut v: Vec<sj::Value> = Vec::new();
        while let Some(emt1) = seq.next_element()? {
            v.push(emt1);
        }

        return JsonUuidVisitor::parse::<A>(sj::Value::Array(v));
    }
}

impl<'de> Visitor<'de> for JsonUuidSetVisitor {
    type Value = Vec<String>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("set of UUIDs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where A: SeqAccess<'de>
    {
        // It's either a ["set", [... UUIDs ...]], or a single UUID, ["uuid",
        // string].  So in any case, the array is supposed to have two elements.
        let emt1: Option<sj::Value> = seq.next_element()?;
        let emt2: Option<sj::Value> = seq.next_element()?;
        if emt1.is_none() || emt2.is_none() {
            return Err(A::Error::invalid_length(0, &"2"));
        }

        match emt1.unwrap() {
            sj::Value::String(ref head) if head == "set" => {
                if let sj::Value::Array(uuids) = emt2.unwrap() {
                    let mut ret = Self::Value::new();
                    for entry in uuids {
                        let uuid = JsonUuidVisitor::parse::<A>(entry)?;
                        ret.push(uuid);
                    }
                    return Ok(ret);
                } else {
                    return Err(A::Error::custom(&"Malformed JSON RPC set"));
                }
            }

            sj::Value::String(ref head) if head == "uuid" => {
                if let sj::Value::String(uuid) = emt2.unwrap() {
                    return Ok(vec![uuid]);
                } else {
                    return Err(A::Error::custom(&"Malformed JSON RPC uuid"));
                }
            }

            _ => {
                return Err(A::Error::custom(&"expected \"set\" or \"uuid\""));
            }
        }
    }
}

fn deserialize_uuid_set<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
    where D: serde::Deserializer<'de>
{
    deserializer.deserialize_seq(JsonUuidSetVisitor)
}

#[derive(Deserialize, Debug)]
struct JsonVtepPhysicalSwitch {
    name: String,
    ports: sj::Value,
    tunnel_ips: String,
    #[serde(deserialize_with = "deserialize_uuid_set")]
    tunnels: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct JsonDiffVtepPhysicalSwitch {
    old: Option<JsonVtepPhysicalSwitch>,
    new: Option<JsonVtepPhysicalSwitch>,
}

#[derive(Deserialize, Debug)]
struct JsonDiff {
    #[serde(rename="Physical_Switch")]
    physical_switch: HashMap<String, JsonDiffVtepPhysicalSwitch>,
}

fn jsonrpc_result(expect_id: u32, result: JsonRpcResult) -> Result<sj::Value, String> {
    if result.error.is_some() {
        let error = result.error.unwrap();
        return Result::Err(format!("JSON RPC error: {}, {}", error.error, error.details));
    }
    if result.id != expect_id {
        return Result::Err(format!("Response ID mismatch: {} should be 0", result.id));
    }
    if result.result.is_none() {
        return Result::Err(format!("Malformed JSON RPC response with neither error, nor result."));
    }
    return Result::Ok(result.result.unwrap());
}

fn jsonrpc_communicate(stream: &mut unix_socket::UnixStream,
                       method: &str, params: sj::Value) -> Result<sj::Value, String> {
    let request = json!({
        "id": 0,
        "method": method,
        "params": params,
    });

    stream.write_all(request.to_string().as_bytes())
        .map_err(|err| format!("Failed to write request: {}", err)) ?;

    for val in sj::Deserializer::from_reader(stream).into_iter::<JsonRpcResult>() {
        let res = val.map_err(|err| format!("JSON RPC error: {}", err)) ?;
        return jsonrpc_result(0, res);
    }
    return Result::Err("No JSON RPC response.".to_string());
}

fn main2() -> Result<(), String> {
    let mut stream = unix_socket::UnixStream::connect("/var/run/openvswitch/db.sock")
        .map_err(|err| format!("Couldn't open OVSDB socket: {}", err)) ?;

    {
        let result = jsonrpc_communicate(&mut stream, "echo", json!(["Hello", "OVSDB", "?"])) ?;
        println!("{}", result);
    }

    {
        let mon = json!(["hardware_vtep", "hardware_vtep",
                         {
                             "Physical_Switch": {
                                 "columns": ["name", "ports", "tunnel_ips", "tunnels"],
                             },
                         }]);
        let result = jsonrpc_communicate(&mut stream, "monitor", mon) ?;
        let d: JsonDiff = sj::from_value(result).unwrap();
        println!("{:?}", d);
    }

    {
        let mon = json!(["Open_vSwitch", "Open_vSwitch",
                         {
                             "Interface": {
                                 "columns": ["name", "type", "ofport"],
                             },
                         }]);
        let result = jsonrpc_communicate(&mut stream, "monitor", mon) ?;
        println!("{}", result);
    }

    Result::Ok(())
}

fn main() {
    match main2() {
        Result::Ok(_) => {},
        Result::Err(e) => println!("{}", e),
    }
}
