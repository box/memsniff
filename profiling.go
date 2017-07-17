// Copyright 2017 Box, Inc.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

func startProfiling() func() {
	if isProfileEnabled("cpu") {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatalln(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalln(err)
		}
	}
	if isProfileEnabled("block") {
		runtime.SetBlockProfileRate(1)
	}

	return dumpProfiles
}

func isProfileEnabled(profile string) bool {
	for _, p := range *profiles {
		if p == profile {
			return true
		}
	}
	return false
}

func dumpProfiles() {
	for _, p := range *profiles {
		dumpProfile(p)
	}
}

func dumpProfile(p string) {
	if p == "cpu" {
		pprof.StopCPUProfile()
		return
	}

	f, err := os.Create(p + ".pprof")
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatalln("Could not close profile", p, err)
		}
	}()

	prof := pprof.Lookup(p)

	if prof != nil {
		// print symbolic names in profile to make them human-readable
		err = prof.WriteTo(f, 1)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
