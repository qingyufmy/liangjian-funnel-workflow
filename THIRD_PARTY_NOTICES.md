# Third-party notices

## A1 research websites

`config/a1_research_sources.yaml` records source roles and access constraints;
it does not grant redistribution, scraping, API, or commercial-use rights.
CNINFO and public government facts are consumed through bounded point-in-time
adapters. Jisilu and Datayes/Robo automated collection is disabled because
their published service terms prohibit crawler/robot use. Paid research from
Mybbond/Hibor, Xingqiao, Datayes/Robo, or any similar service may enter only as
an operator-reviewed export the operator is authorized to use. iFinD remains
disabled until a separately licensed data-interface account is configured.
Public X posts from `cnfinancewatch` are recorded only as manually reviewed T3
methodology references. The workflow does not automate X collection, reproduce
the posts as a data feed, or treat their quoted news, research, market data,
position suggestions, or named stocks as verified facts or selection authority.

## Vibe-Research

The open-news source catalog and the design of the RSS/news adapters are
derived from [simonlin1212/Vibe-Research](https://github.com/simonlin1212/Vibe-Research),
which is distributed under the MIT License.

Copyright (c) 2026 simonlin1212

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

The workflow treats every media/RSS payload as untrusted T3 material. Source
inclusion in the catalog is not an endorsement and does not make an article a
company filing, policy document, or verified fundamental fact.

## Serenity.skill

The A2 supply-chain layer ranking, bottleneck factor vocabulary, evidence
ladder, and failure-condition research pattern are adapted from
[muxuuu/serenity-skill](https://github.com/muxuuu/serenity-skill) at commit
`c2fe93deedfd0d1bd9fe7ef0601ea1b9c20ea24a`, distributed under the MIT
License.

Copyright (c) 2026 muxu

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Liangjian does not treat the upstream repository's example companies as
recommendations or runtime input. Every A2 focus candidate must come from the
current A1 pool and pass the frozen-evidence and lineage checks locally.

## AKShare

Open macro, rate, ETF and National Bureau of Statistics normalization uses
[AKShare](https://github.com/akfamily/akshare) version `1.18.94`, distributed
under the MIT License. AKShare is an aggregation and normalization adapter,
not an authority tier by itself; the workflow records the underlying endpoint
and degrades rather than inventing unavailable observations.
