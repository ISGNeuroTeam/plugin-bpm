# OT.BPM plugin

## Installation

Extract package to `$OTP_HOME/dispatcher/plugins/plugin-bpm`.

## Configuration

1. Find `'plugin.conf` file in `$OTP_HOME/dispatcher/plugins/smallplugin-core` of your installation of OT.Platform.
2. Rename `docs/bpm.conf.example` to `bpm.conf`. Check path from `configBasePath` parameter of your `plugin.conf`. Locate this path and put `bpm.conf` there.
3. Merge `apply` section of `plugin.conf.example` with `apply` section of your `plugin.conf`. 
4. Restart dispatcher.

## Authors

From [ISGNeuro DS Team](https://github.com/orgs/ISGNeuroTeam/teams/ds):

- Nikolai Streletskii
- Ivan Temirov
- Nikolai Riabykh
