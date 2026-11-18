package main

import fraudscore "omnifoni/fraud/scoring"

const RuleVersion = fraudscore.RuleVersion

type Config = fraudscore.Config

type Signals = fraudscore.Signals

func DestinationPrefix(destination string) string {
	return fraudscore.DestinationPrefix(destination)
}

func Score(cfg Config, s Signals) (int, []string) {
	return fraudscore.Score(cfg, s)
}

func Level(score int) string {
	return fraudscore.Level(score)
}
