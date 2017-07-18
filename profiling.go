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
		if err = f.Close(); err != nil {
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
