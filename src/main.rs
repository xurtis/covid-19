use serde::Deserialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::iter::{FromIterator, Sum, once};
use structopt::StructOpt;

const CONFIRMED_URL: &'static str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv";
const DEATHS_URL: &'static str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv";
const RECOVERED_URL: &'static str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv";

type Error = Box<dyn std::error::Error>;
type Result<T> = ::std::result::Result<T, Error>;

/// Query the WHO COVID-19 timeseries
#[derive(StructOpt)]
enum Args {
    /// List the countries and regions and their states and provinces
    List,
    /// Query the time-series for a particular population
    Query {
        /// Display changes per day rather than totals
        #[structopt(long, short)]
        changes: bool,
        /// Query a particular region or country
        region: Option<String>,
        /// Query a particular province within the country
        province: Option<String>,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args();

    let confirmed = DataSet::download(CONFIRMED_URL).await?;
    let deaths = DataSet::download(DEATHS_URL).await?;
    let recovered = DataSet::download(RECOVERED_URL).await?;

    let joined = DataSet::join(&confirmed, &deaths, &recovered);

    match args {
        Args::List => {
            println!("World");
            for (name, region) in joined.regions() {
                println!(" - {}", name);
                for (name, _) in region.provinces() {
                    println!("    - {}", name);
                }
            }
        }
        Args::Query { changes, region: None, province: None } => {
            println!("World");
            println!("=====\n");

            if changes {
                show_dates(true, deltas(joined.totals()));
            } else {
                show_dates(false, joined.totals());
            }

            println!();
        }
        Args::Query { changes, region: Some(region), province: None } => {
            let dates = joined.region(&region).expect("Invalid region");

            println!("{}", region);
            println!("{}\n", "=".repeat(region.len()));

            if changes {
                show_dates(true, deltas(dates.totals()));
            } else {
                show_dates(false, dates.totals());
            }

            println!();
        }
        Args::Query { changes, region: Some(region), province: Some(province) } => {
            let dates = joined.region(&region).expect("Invalid region");
            let dates = dates.province(&province).expect("Invalid province");

            println!("{} / {}", region, province);
            println!("{}\n", "=".repeat(region.len() + 3 + province.len()));

            if changes {
                show_dates(true, deltas(dates.totals()));
            } else {
                show_dates(false, dates.totals());
            }

            println!();
        }
        _ => unreachable!()
    }

    Ok(())
}

fn show_dates<T: fmt::Display>(delta: bool, dates: impl Iterator<Item = (Date, T)>) {
    for (date, count) in dates {
        if delta {
            println!("    {} (Δ): {}", date, count);
        } else {
            println!("    {}: {}", date, count);
        }
    }
}

#[derive(Debug, Deserialize)]
struct Location {
    /// Country or region
    #[serde(rename = "Country/Region")]
    region: String,
    /// Province or state
    #[serde(rename = "Province/State")]
    province: String,
    /// Latitude
    #[serde(rename = "Lat")]
    latitude: f32,
    /// Longitude
    #[serde(rename = "Long")]
    longitude: f32,
    /// Fields
    #[serde(flatten)]
    histogram: BTreeMap<Date, f64>,
}

#[derive(Debug, Clone)]
struct DataSet<T>(BTreeMap<String, Region<T>>);

impl DataSet<f64> {
    async fn download(url: &str) -> Result<Self> {
        let text = reqwest::get(url).await?.text().await?;
        let mut rows = csv::ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(true)
            .flexible(false)
            .from_reader(text.as_bytes());
        Ok(rows.deserialize::<Location>().flat_map(|l| l.ok()).collect())
    }
}

impl DataSet<Counts> {
    fn join(
        DataSet(confirmed): &DataSet<f64>,
        DataSet(deaths): &DataSet<f64>,
        DataSet(recovered): &DataSet<f64>,
    ) -> Self {
        let mut regions = BTreeMap::new();
        for (name, confirmed) in confirmed.iter() {
            let deaths = &deaths[name];
            let recovered = &recovered[name];
            regions.insert(
                name.to_owned(),
                Region::join(confirmed, deaths, recovered),
            );
        }
        DataSet(regions)
    }
}

impl<T> DataSet<T> {
    fn regions(&self) -> impl Iterator<Item = (&str, &Region<T>)> {
        self.0.iter().map(|(name, region)| (name.as_str(), region))
    }

    fn region(&self, region: &str) -> Option<&Region<T>> {
        self.0.get(region)
    }
}

impl<'s, T: Sum + Copy + 's> Totals<'s, T> for DataSet<T> {
    fn sources(&'s self) -> Box<dyn Iterator<Item = BTreeMap<Date, T>> + 's> {
        Box::new(self.0.values().map(|r| r.totals().collect()))
    }
}

impl FromIterator<Location> for DataSet<f64> {
    fn from_iter<T: IntoIterator<Item = Location>>(iter: T) -> Self {
        let mut regions = BTreeMap::new();

        for item in iter.into_iter() {
            let position = Position::from(&item);
            if item.province == "" {
                let region = Region::Singluar { position, data: item.histogram };
                regions.insert(item.region, region);
            } else if let Region::Provinces(provinces) = regions.entry(item.region).or_insert(provinces()) {
                let province = Province { position, data: item.histogram };
                provinces.insert(item.province, province);
            }
        }

        DataSet(regions)
    }
}

#[derive(Debug, Clone)]
enum Region<T> {
    Provinces(BTreeMap<String, Province<T>>),
    Singluar {
        position: Position,
        data: BTreeMap<Date, T>,
    },
}

impl<T> Region<T> {
    fn provinces<'s>(&'s self) -> Box<dyn Iterator<Item = (&'s str, &'s Province<T>)> + 's> {
        match self {
            Region::Provinces(provinces) => {
                Box::new(provinces.iter().map(|(name, province)| (name.as_str(), province)))
            }
            Region::Singluar { .. } => Box::new(None.into_iter()),
        }
    }

    fn province(&self, province: &str) -> Option<&Province<T>> {
        match self {
            Region::Provinces(provinces) => provinces.get(province),
            Region::Singluar { .. } => None,
        }
    }
}

impl<'s, T: Sum + Copy + 's> Totals<'s, T> for Region<T> {
    fn sources(&'s self) -> Box<dyn Iterator<Item = BTreeMap<Date, T>> + 's> {
        match self {
            Region::Provinces(provinces) => {
                Box::new(provinces.values().map(|p| p.data.clone()))
            }
            Region::Singluar { data, .. } => {
                Box::new(once(data.clone()))
            }
        }
    }
}

impl Region<Counts> {
    fn join(
        confirmed: &Region<f64>,
        deaths: &Region<f64>,
        recovered: &Region<f64>,
    ) -> Self {
        use Region::*;
        match (confirmed, deaths, recovered) {
            (Provinces(confirmed), Provinces(deaths), Provinces(recovered)) => {
                let mut provinces = BTreeMap::new();
                for (name, confirmed) in confirmed.iter() {
                    let deaths = &deaths[name];
                    let recovered = &recovered[name];
                    provinces.insert(
                        name.to_owned(),
                        Province::join(confirmed, deaths, recovered),
                    );
                }
                Provinces(provinces)
            }
            (
                Singluar { position, data: confirmed },
                Singluar { data: deaths, .. },
                Singluar { data: recovered, .. },
            ) => {
                let position = *position;
                let mut data = BTreeMap::new();
                for (date, confirmed) in confirmed.iter() {
                    let confirmed = *confirmed;
                    let deaths = deaths[date];
                    let recovered = recovered[date];
                    data.insert(*date, Counts { confirmed, deaths, recovered });
                }
                Singluar { position, data }
            }
            (confirmed, deaths, recovered) => {
                panic!("Mismatched regions: {:?} {:?} {:?}", confirmed, deaths, recovered);
            }
        }
    }
}

fn provinces<T>() -> Region<T> {
    Region::Provinces(BTreeMap::new())
}

#[derive(Debug, Clone)]
struct Province<T> {
    position: Position,
    data: BTreeMap<Date, T>,
}

impl Province<Counts> {
    fn join(
        confirmed: &Province<f64>,
        deaths: &Province<f64>,
        recovered: &Province<f64>,
    ) -> Self {
        let position = confirmed.position;
        let mut data = BTreeMap::new();
        for (date, confirmed) in confirmed.data.iter() {
            let confirmed = *confirmed;
            let deaths = deaths.data[date];
            let recovered = recovered.data[date];
            data.insert(*date, Counts { confirmed, deaths, recovered });
        }
        Province { position, data }
    }
}

impl<'s, T: Sum + Copy + 's> Totals<'s, T> for Province<T> {
    fn sources(&'s self) -> Box<dyn Iterator<Item = BTreeMap<Date, T>> + 's> {
        Box::new(once(self.data.clone()))
    }
}

#[derive(Debug, Clone, Copy)]
struct Position {
    latitude: f32,
    longitude: f32,
}

impl From<&Location> for Position {
    fn from(location: &Location) -> Self {
        Position {
            latitude: location.latitude,
            longitude: location.longitude,
        }
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(try_from="String")]
struct Date {
    year: u32,
    month: u32,
    day: u32,
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year + 2000, self.month, self.day)
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug)]
struct DateError(String);

impl fmt::Display for DateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid date string: {}", self.0)
    }
}

impl std::error::Error for DateError {}

impl TryFrom<String> for Date {
    type Error = Error;

    fn try_from(text: String) -> Result<Self> {
        if let [month, day, year] = &text.trim().split("/").collect::<Vec<_>>()[..] {
            let year = year.parse()?;
            let month = month.parse()?;
            let day = day.parse()?;
            Ok(Date { year, month, day })
        } else {
            Err(DateError(text).into())
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Counts {
    confirmed: f64,
    deaths: f64,
    recovered: f64,
}

impl Counts {
    const ZERO: Self = Counts {
        confirmed: 0.0,
        deaths: 0.0,
        recovered: 0.0,
    };
}

impl std::ops::Add for Counts {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Counts {
            confirmed: self.confirmed + other.confirmed,
            deaths: self.deaths + other.deaths,
            recovered: self.recovered + other.recovered,
        }
    }
}

impl std::ops::Sub for Counts {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Counts {
            confirmed: self.confirmed - other.confirmed,
            deaths: self.deaths - other.deaths,
            recovered: self.recovered - other.recovered,
        }
    }
}

impl std::ops::Div for Counts {
    type Output = Self;
    fn div(self, other: Self) -> Self {
        Counts {
            confirmed: self.confirmed / other.confirmed,
            deaths: self.deaths / other.deaths,
            recovered: self.recovered / other.recovered,
        }
    }
}

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, std::ops::Add::add)
    }
}

impl fmt::Display for Counts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "🤒 {:8.2} / ☠️  {:8.2} / 😌 {:8.2}",
            self.confirmed,
            self.deaths,
            self.recovered,
        )
    }
}

trait Totals<'s, T: Sum + Copy + 's> {
    fn sources(&'s self) -> Box<dyn Iterator<Item = BTreeMap<Date, T>> + 's>;

    fn totals(&'s self) -> Box<dyn Iterator<Item = (Date, T)> + 's> {
        let sources = self.sources().collect::<Vec<_>>();
        let dates = sources
            .iter()
            .next()
            .map(|counts| counts.keys().cloned().collect::<Vec<_>>());
        let iter = dates
            .into_iter()
            .flat_map(|dates| dates.into_iter())
            .map(move |date| (date, sources.iter().map(|source| source[&date]).sum::<T>()));
        Box::new(iter)
    }
}

fn deltas<T: std::ops::Sub + Copy>(
    counts: impl Iterator<Item = (Date, T)>
) -> impl Iterator<Item = (Date, T::Output)> {
    step_with(counts, |prev, next| {
        let (_, prev) = prev;
        let (date, next) = next;
        (date, next - prev)
    })
}

#[cfg(not)]
fn ratios<T: std::ops::Div + Copy>(
    counts: impl Iterator<Item = (Date, T)>
) -> impl Iterator<Item = (Date, T::Output)> {
    step_with(counts, |prev, next| {
        let (_, prev) = prev;
        let (date, next) = next;
        (date, next / prev)
    })
}

fn step_with<I, T, O, F>(mut iter: I, step: F) -> impl Iterator<Item = O>
where
    T: Copy,
    I: Iterator<Item = T>,
    F: Fn(T, T) -> O,
{
    let mut prev = iter.next();
    let step = move |next| {
        let mut value = None;
        std::mem::swap(&mut value, &mut prev);
        let value = step(value.unwrap(), next);
        prev = Some(next);
        value
    };
    iter.map(step)
}
