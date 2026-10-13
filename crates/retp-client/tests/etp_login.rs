//! The logins, where they can be checked without a network: the header an
//! OAuth client authenticates with, Cognito's `SECRET_HASH`, and how each
//! authorization server's answer is read.
//!
//! Every expected value comes from outside this crate: the examples of
//! RFC 6749 and RFC 6750, the shapes of AWS's `InitiateAuth` reference, and
//! an HMAC worked out with Python's `hmac` - never from the code under test.

use std::time::Duration;

use retp_client::login::{
    Choice, Login, LoginError, azure_token_url, client_secret_basic, client_secret_post, cognito_secret_hash,
    password_grant_form, read_cognito_answer, read_oauth_answer,
};
use retp_client::native::CertificateCheck;
use retp_proto::credentials::Credentials;

/// RFC 6749 §2.3.1's own example, and its Appendix B: id and secret are
/// form-encoded **before** Basic, so a `:` in a secret cannot be taken for
/// the separator.
#[test]
fn client_secret_basic_is_rfc_6749s() {
    let example = client_secret_basic("s6BhdRkqt3", "7Fjfp0ZBr1KtDRbnfVdmIw").unwrap();
    assert_eq!(
        example.header(),
        "Basic czZCaGRSa3F0Mzo3RmpmcDBaQnIxS3REUmJuZlZkbUl3"
    );
    let encoded = client_secret_basic("a b", "c:d").unwrap();
    assert_eq!(encoded.header(), "Basic YStiOmMlM0Fk", "`a+b:c%3Ad`");
}

/// `Base64(HMAC_SHA256(secret, user + client_id))`, against Python's `hmac`.
#[test]
fn cognito_secret_hash_is_the_hmac_aws_asks_for() {
    assert_eq!(
        cognito_secret_hash("user@example.com", "1example23456789clientid", "k3y-s3cr3t"),
        "aZxi5EjYnA1+QwtzdQXg+4HzrY6vZwA0Iz42GLxczuE="
    );
}

/// RFC 6750 §4's answer gives its token; RFC 6749 §5.1's, whose type is
/// `example`, is refused rather than sent as a bearer token; §5.2's error is
/// named; and a gateway's HTML page is not taken for an answer.
#[test]
fn an_oauth_answer_is_read_as_the_rfcs_write_it() {
    let rfc_6750 = br#"{"access_token":"mF_9.B5f-4.1JqM","token_type":"Bearer","expires_in":3600,"refresh_token":"tGzv3JOkF0XG5Qx2TlKWIA"}"#;
    let obtained = read_oauth_answer("https://server.example.com/token", 200, rfc_6750).unwrap();
    assert_eq!(
        obtained.credentials,
        Credentials::bearer("mF_9.B5f-4.1JqM").unwrap()
    );
    assert_eq!(obtained.expires_in, Some(Duration::from_secs(3600)));

    let rfc_6749 = br#"{"access_token":"2YotnFZFEjr1zCsicMWpAA","token_type":"example","expires_in":3600}"#;
    match read_oauth_answer("s", 200, rfc_6749) {
        Err(LoginError::Invalid(why)) => {
            assert!(why.contains("`example`") && why.contains("bearer"), "{why}")
        }
        other => panic!("{other:?}"),
    }

    match read_oauth_answer("s", 400, br#"{"error":"invalid_request"}"#) {
        Err(LoginError::Refused { status, error, .. }) => {
            assert_eq!((status, error.as_str()), (400, "invalid_request"))
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        read_oauth_answer("s", 502, b"<html>Bad Gateway</html>"),
        Err(LoginError::Http(_))
    ));
}

/// The three shapes of AWS's `InitiateAuth` reference: the **access** token
/// is the one taken - not the id token beside it -, a challenge is named, and
/// an error keeps its type.
#[test]
fn a_cognito_answer_is_read_as_aws_writes_it() {
    let granted = br#"{"AuthenticationResult":{"AccessToken":"acc.ess","ExpiresIn":3600,"IdToken":"id.token","RefreshToken":"ref","TokenType":"Bearer"},"ChallengeParameters":{}}"#;
    let obtained = read_cognito_answer(200, granted).unwrap();
    assert_eq!(obtained.credentials.header(), "Bearer acc.ess");
    assert_eq!(obtained.expires_in, Some(Duration::from_secs(3600)));

    let challenge = br#"{"ChallengeName":"NEW_PASSWORD_REQUIRED","ChallengeParameters":{},"Session":"s"}"#;
    assert_eq!(
        read_cognito_answer(200, challenge),
        Err(LoginError::Challenge {
            name: "NEW_PASSWORD_REQUIRED".into()
        })
    );

    let refused = br#"{"__type":"NotAuthorizedException","message":"Incorrect username or password."}"#;
    match read_cognito_answer(400, refused) {
        Err(LoginError::Refused {
            error, description, ..
        }) => {
            assert_eq!(error, "NotAuthorizedException");
            assert_eq!(description, "Incorrect username or password.");
        }
        other => panic!("{other:?}"),
    }
}

/// What goes into a host name is checked before any request leaves.
#[test]
fn what_names_a_host_is_checked_before_asking() {
    assert_eq!(
        azure_token_url("58975fd3-0000-44d0-bea8-000000000000").unwrap(),
        "https://login.microsoftonline.com/58975fd3-0000-44d0-bea8-000000000000/oauth2/v2.0/token"
    );
    assert!(azure_token_url("tenant/../x").is_err());
    let cognito = Login::Cognito {
        region: "us-east-2.evil.example/".into(),
        client_id: "c".into(),
        client_secret: None,
        user: "u".into(),
        password: "p".into(),
    };
    assert!(matches!(cognito.obtain(), Err(LoginError::Invalid(why)) if why.contains("AWS region")));
    // And the credentials that need no server come out at once.
    let basic = Login::Basic {
        user: "Aladdin".into(),
        password: "open sesame".into(),
    };
    assert_eq!(
        basic.obtain().unwrap().credentials.header(),
        "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
    );
}

/// Turning the certificate check off is never silent.
#[test]
fn an_unchecked_certificate_is_said() {
    assert_eq!(CertificateCheck::Verify.warning("wss://h/etp"), None);
    assert_eq!(CertificateCheck::default(), CertificateCheck::Verify);
    let warning = CertificateCheck::Skip.warning("wss://h/etp").unwrap();
    assert!(
        warning.contains("wss://h/etp") && warning.contains("not checked"),
        "{warning}"
    );
}

/// RFC 6749 §4.3.2's own request: `grant_type=password&username=johndoe&password=A3ddj3w`.
/// A public client (an id, no secret) names itself in the form (§3.2.1); a
/// client with a secret authenticates in the header instead, and its id
/// stays out of the form.
#[test]
fn a_password_grant_is_rfc_6749s() {
    let form = password_grant_form("johndoe", "A3ddj3w", None, false, None);
    assert_eq!(
        form,
        vec![
            ("grant_type", "password".to_string()),
            ("username", "johndoe".to_string()),
            ("password", "A3ddj3w".to_string())
        ]
    );
    let public = password_grant_form("johndoe", "A3ddj3w", Some("s6BhdRkqt3"), false, Some("read"));
    assert!(
        public.contains(&("client_id", "s6BhdRkqt3".to_string()))
            && public.contains(&("scope", "read".to_string()))
    );
    let confidential = password_grant_form("johndoe", "A3ddj3w", Some("s6BhdRkqt3"), true, None);
    assert!(!confidential.iter().any(|(key, _)| *key == "client_id"));
}

/// Which login a command line asks for, its secrets read from the
/// environment: the faces parse, `Choice::login` decides.
#[test]
fn the_login_is_chosen_in_one_place() {
    let env = |pairs: &'static [(&'static str, &'static str)]| {
        move |name: &str| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
    };
    let url = Some("https://auth.example/token".to_string());
    // A token endpoint and a user: the password grant.
    let password = Choice {
        token_url: url.clone(),
        ..Default::default()
    }
    .login(env(&[("ETP_USER", "u"), ("ETP_PASSWORD", "p")]))
    .unwrap();
    assert!(matches!(password, Some(Login::Password { user, .. }) if user == "u"));
    // A token endpoint and no user: client credentials.
    let client = Choice {
        token_url: url.clone(),
        client_id: Some("c".into()),
        ..Default::default()
    }
    .login(env(&[("ETP_CLIENT_SECRET", "s")]))
    .unwrap();
    assert!(matches!(client, Some(Login::ClientCredentials { client_id, .. }) if client_id == "c"));
    // A user alone: Basic.
    let basic = Choice::default()
        .login(env(&[("ETP_USER", "u"), ("ETP_PASSWORD", "p")]))
        .unwrap();
    assert!(matches!(basic, Some(Login::Basic { user, .. }) if user == "u"));
    // Nothing but a token in the environment: that token.
    let bearer = Choice::default().login(env(&[("ETP_TOKEN", "t")])).unwrap();
    assert!(matches!(bearer, Some(Login::Bearer(token)) if token == "t"));
    // A token given wins over a user in the environment.
    let given = Choice {
        token: Some("t".into()),
        ..Default::default()
    }
    .login(env(&[("ETP_USER", "u")]))
    .unwrap();
    assert!(matches!(given, Some(Login::Bearer(_))));
    // A user without a password is said, not sent.
    assert!(
        matches!(Choice::default().login(env(&[("ETP_USER", "u")])), Err(LoginError::Invalid(why)) if why.contains("password"))
    );
    // And nothing at all is no login.
    assert!(Choice::default().login(env(&[])).unwrap().is_none());
}

/// `client_secret_post` (RFC 6749 §2.3.1): the id and secret in the form,
/// as the spec's own example writes them (`client_id=s6BhdRkqt3&client_secret=7Fjfp0ZBr1KtDRbnfVdmIw`),
/// unencoded before the form's own encoding - what a server that does not
/// decode Basic credentials takes. An id already in the form is not doubled.
#[test]
fn client_secret_post_puts_the_client_in_the_form() {
    let form = client_secret_post(
        vec![("grant_type", "client_credentials".to_string())],
        "s6BhdRkqt3",
        "7Fjfp0ZBr1KtDRbnfVdmIw",
    );
    assert_eq!(
        form,
        vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", "s6BhdRkqt3".to_string()),
            ("client_secret", "7Fjfp0ZBr1KtDRbnfVdmIw".to_string())
        ]
    );
    let public = password_grant_form("johndoe", "A3ddj3w", Some("s6BhdRkqt3"), false, None);
    let posted = client_secret_post(public, "s6BhdRkqt3", "x");
    assert_eq!(posted.iter().filter(|(key, _)| *key == "client_id").count(), 1);
}
