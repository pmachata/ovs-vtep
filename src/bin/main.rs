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

#[derive(Serialize, Debug)]
struct JsonRpcMonitorEventParams {
    key: sj::Value,
    updates: sj::Value,
}

struct JsonRpcMonitorEventParamsVisitor;

impl<'de> Visitor<'de> for JsonRpcMonitorEventParamsVisitor {
    type Value = JsonRpcMonitorEventParams;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Two-element array with monitor even parameters")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where A: SeqAccess<'de>
    {
        let emt1: Option<sj::Value> = seq.next_element()?;
        let emt2: Option<sj::Value> = seq.next_element()?;
        if emt1.is_none() || emt2.is_none() {
            return Err(A::Error::invalid_length(0, &"2"));
        }

        return Ok(JsonRpcMonitorEventParams {key: emt1.unwrap(),
                                             updates: emt2.unwrap()});
    }
}

fn deserialize_monitor_event_params<'de, D>(deserializer: D)
    -> Result<JsonRpcMonitorEventParams, D::Error>
    where D: serde::Deserializer<'de>
{
    deserializer.deserialize_seq(JsonRpcMonitorEventParamsVisitor)
}

#[derive(Serialize, Deserialize)]
struct JsonRpcMonitorEvent {
    id: (),
    method: String,
    #[serde(deserialize_with = "deserialize_monitor_event_params")]
    params: JsonRpcMonitorEventParams,
}

struct JsonUuidVisitor;

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

struct JsonUuidSetVisitor;

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

fn deserialize_uuid_set<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
    where D: serde::Deserializer<'de>
{
    Ok(Some(deserializer.deserialize_seq(JsonUuidSetVisitor)?))
}

fn deserialize_uuid<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where D: serde::Deserializer<'de>
{
    Ok(Some(deserializer.deserialize_seq(JsonUuidVisitor)?))
}

// Note regarding typing: This should be reasonably close to the actual OVSDB
// schema.  So keep things as strings, even if they should eventually represent
// e.g. IP addresses, or UUIDs (xxx check--this might actually be guaranteed),
// unless OVSDB guarantees that a given field actually always contains a, say,
// IP and one can't smuggle in anything else.

#[derive(Deserialize, Debug)]
struct JsonVtepPhysicalSwitchPart {
    name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_uuid_set")]
    ports: Option<Vec<String>>,
    tunnel_ips: Option<String>,
    #[serde(default, deserialize_with = "deserialize_uuid_set")]
    tunnels: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct JsonVtepPhysicalLocatorPart {
    dst_ip: Option<String>,
    encapsulation_type: Option<String>,
}

#[derive(Deserialize, Debug)]
struct JsonVtepPhysicalLocatorSetPart {
    locators: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct JsonVtepTunnelPart {
    #[serde(default, deserialize_with = "deserialize_uuid")]
    local: Option<String>,
    #[serde(default, deserialize_with = "deserialize_uuid")]
    remote: Option<String>,
}

#[derive(Deserialize, Debug)]
struct JsonDiffVtepPhysicalSwitch {
    old: Option<JsonVtepPhysicalSwitchPart>,
    new: Option<JsonVtepPhysicalSwitchPart>,
}

#[derive(Deserialize, Debug)]
struct JsonDiffVtepPhysicalLocator {
    old: Option<JsonVtepPhysicalLocatorPart>,
    new: Option<JsonVtepPhysicalLocatorPart>,
}

#[derive(Deserialize, Debug)]
struct JsonDiffVtepPhysicalLocatorSet {
    old: Option<JsonVtepPhysicalLocatorSetPart>,
    new: Option<JsonVtepPhysicalLocatorSetPart>,
}

#[derive(Deserialize, Debug)]
struct JsonDiffVtepTunnel {
    old: Option<JsonVtepTunnelPart>,
    new: Option<JsonVtepTunnelPart>,
}

#[derive(Deserialize, Debug)]
struct JsonDiff {
    #[serde(default, rename="Physical_Switch")]
    physical_switch: HashMap<String, JsonDiffVtepPhysicalSwitch>,
    #[serde(default, rename="Physical_Locator")]
    physical_locator: HashMap<String, JsonDiffVtepPhysicalLocator>,
    #[serde(default, rename="Physical_Locator_Set")]
    physical_locator_set: HashMap<String, JsonDiffVtepPhysicalLocatorSet>,
    #[serde(default, rename="Tunnel")]
    tunnel: HashMap<String, JsonDiffVtepTunnel>,
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
                             "Physical_Locator": {
                                 "columns": ["dst_ip", "encapsulation_type"],
                             },
                             "Physical_Locator_Set": {
                                 "columns": ["locators"],
                             },
                             "Tunnel": {
                                 "columns": ["local", "remote"],
                             }
                         }]);
        let result = jsonrpc_communicate(&mut stream, "monitor", mon) ?;
        println!("{:?}\n---", result);
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

    for val in sj::Deserializer::from_reader(stream).into_iter::<JsonRpcMonitorEvent>() {
        let res = val.map_err(|err| format!("JSON RPC error: {}", err)) ?;
        if let sj::Value::String(ref dbname) = res.params.key {
            if dbname == "hardware_vtep" {
                let d: JsonDiff = sj::from_value(res.params.updates).unwrap();
                println!("VTEP update: {:?}", d);
            } else if dbname == "Open_vSwitch" {
                println!("OVS update: {:?}", res.params.updates);
            } else {
                return Err(format!("Monitor event relating to an unknown database {}", dbname));
            }
        } else {
            return Err(format!("Invalid monitor event key: {}", res.params.key));
        }
    }

    Result::Ok(())
}

fn main() {
    match main2() {
        Result::Ok(_) => {},
        Result::Err(e) => println!("{}", e),
    }
}
