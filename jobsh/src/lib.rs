/*
 * Copyright 2024 Oxide Computer Company
 */

use std::borrow::Cow;

use buildomat_client::types::JobEvent;
use chrono::SecondsFormat;
use serde::Serialize;

pub mod jobfile;
pub mod variety;

/*
 * Classes for these streams are defined in the "variety/basic/www/style.css",
 * which we send along with the generated HTML output.
 */
const CSS_STREAM_CLASSES: &[&str] =
    &["stdout", "stderr", "task", "worker", "control", "console"];

pub trait JobEventEx {
    /**
     * Choose a colour (CSS class name) for the stream to which this event
     * belongs.
     */
    fn css_class(&self) -> String;

    /**
     * Turn a job event into a somewhat abstract object with pre-formatted HTML
     * values that we can either use for server-side or client-side (live)
     * rendering.
     */
    fn event_row(&self) -> EventRow;
}

impl JobEventEx for JobEvent {
    fn css_class(&self) -> String {
        let s = self.stream.as_str();

        if CSS_STREAM_CLASSES.contains(&s) {
            format!("s_{s}")
        } else if s.starts_with("bg.") {
            "s_bgtask".into()
        } else {
            "s_default".into()
        }
    }

    fn event_row(&self) -> EventRow {
        let payload = format!(
            "{}{}",
            /*
             * If this is a background task, prefix the output with the
             * user-provided name of that task:
             */
            self.stream
                .strip_prefix("bg.")
                .and_then(|s| s.split_once('.'))
                .filter(|(_, s)| *s == "stdout" || *s == "stderr")
                .map(|(n, _)| format!("[{}] ", html_escape::encode_safe(n)))
                .as_deref()
                .unwrap_or(""),
            /*
             * Do the HTML escaping of the payload one canonical way, in the
             * server:
             */
            encode_payload(&self.payload),
        );

        EventRow {
            task: self.task,
            css_class: self.css_class(),
            fields: vec![
                EventField {
                    css_class: "num",
                    local_time: false,
                    value: self.seq.to_string(),
                    anchor: Some(format!("S{}", self.seq)),
                },
                EventField {
                    css_class: "field",
                    local_time: false,
                    value: self
                        .time
                        .to_rfc3339_opts(SecondsFormat::Millis, true),
                    anchor: None,
                },
                EventField {
                    css_class: "field",
                    local_time: true,
                    value: self
                        .time_remote
                        .map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true))
                        .unwrap_or_else(|| "&nbsp;".into()),
                    anchor: None,
                },
                EventField {
                    css_class: "payload",
                    local_time: false,
                    value: payload,
                    anchor: None,
                },
            ],
        }
    }
}

fn encode_payload(payload: &str) -> Cow<'_, str> {
    /*
     * Apply ANSI formatting to the payload after escaping it (we want to
     * transmit the corresponding HTML tags over the wire).
     *
     * One of the cases this does not handle is multi-line color output split
     * across several payloads.  Doing so is quite tricky, because buildomat
     * works with a single bash script and doesn't know when commands are
     * completed.  Other systems like GitHub Actions (as checked on 2024-09-03)
     * don't handle multiline color either, so it's fine to punt on that.
     */
    ansi_to_html::convert_with_opts(
        payload,
        &ansi_to_html::Opts::default()
            .four_bit_var_prefix(Some("ansi-".to_string())),
    )
    .map_or_else(
        |_| {
            /*
             * Invalid ANSI code: only escape HTML in case the conversion to
             * ANSI fails.  To maintain consistency we use the same logic as
             * ansi-to-html: do not escape "/".  (There are other differences,
             * such as ansi-to-html using decimal escapes while html_escape uses
             * hex, but those are immaterial.)
             */
            html_escape::encode_quoted_attribute(payload)
        },
        Cow::Owned,
    )
}

#[derive(Debug, Serialize)]
pub struct EventRow {
    task: Option<u32>,
    css_class: String,
    fields: Vec<EventField>,
}

#[derive(Debug, Serialize)]
pub struct EventField {
    css_class: &'static str,
    local_time: bool,
    value: String,

    /**
     * This field is a permalink anchor, with this anchor ID:
     */
    anchor: Option<String>,
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_encode_payload() {
        let data = &[
            ("Hello, world!", "Hello, world!"),
            /*
             * HTML escapes:
             */
            (
                "2 & 3 < 4 > 5 / 6 ' 7 \" 8",
                "2 &amp; 3 &lt; 4 &gt; 5 / 6 &#39; 7 &quot; 8",
            ),
            /*
             * ANSI color codes:
             */
            (
                /*
                 * Basic 16-color example; also tests a bright color (96).
                 * (ansi-to-html 0.2.1 claims not to support bright colors, but
                 * it actually does.)
                 */
                "\x1b[31mHello, world!\x1b[0m \x1b[96mAnother message\x1b[0m",
                "<span style='color:var(--ansi-red,#a00)'>Hello, world!</span> \
                <span style='color:var(--ansi-bright-cyan,#5ff)'>\
                Another message</span>",
            ),
            (
                /*
                 * Truecolor, bold, italic, underline, and also with escapes.
                 * The second code ("another") does not have a reset, but we
                 * want to ensure that we generate closing HTML tags anyway.
                 */
                "\x1b[38;2;255;0;0;1;3;4mTest message\x1b[0m and &/' \
                \x1b[38;2;0;255;0;1;3;4manother",
                "<span style='color:#ff0000'><b><i><u>Test message</u></i></b>\
                </span> and &amp;/&#39; <span style='color:#00ff00'><b><i>\
                <u>another</u></i></b></span>",
            ),
            (
                /*
                 * Invalid ANSI code "xx"; should be HTML-escaped but the
                 * invalid ANSI code should remain as-is.  (The second ANSI code
                 * is valid, and ansi-to-html should handle it.)
                 */
                "\x1b[xx;2;255;0;0;1;3;4mTest message\x1b[0m and &/' \
                \x1b[38;2;0;255;0;1;3;4manother",
                "\u{1b}[xx;2;255;0;0;1;3;4mTest message and &amp;/&#39; <span \
                style='color:#00ff00'><b><i><u>another</u></i></b></span>",
            ),
            (
                /*
                 * Invalid ANSI code "9000"; should be HTML-escaped but the
                 * invalid ANSI code should remain as-is.  (The second ANSI code
                 * is valid, but ansi-to-html's current behavior is to error out
                 * in this case.  This can probably be improved.)
                 */
                "\x1b[9000;2;255;0;0;1;3;4mTest message\x1b[0m and &/' \
                \x1b[38;2;0;255;0;1;3;4manother",
                "\u{1b}[9000;2;255;0;0;1;3;4mTest message\u{1b}[0m and \
                &amp;/&#x27; \u{1b}[38;2;0;255;0;1;3;4manother",
            )
        ];

        for (input, expected) in data {
            let output = encode_payload(input);
            assert_eq!(
                output, *expected,
                "output != expected: input: {:?}",
                input
            );
        }
    }
}
