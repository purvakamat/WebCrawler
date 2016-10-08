POSITIVE_TERMS = ["basket", "ball", "basket_ball", "nba", "national", "basketball", "association",
                  "dribble", "johnny", "kerr", "detroit", "pistons", "foul", "alex", "hannum",
                 "attles", "orlando", "magic", "billy", "cunningham", "san", "antonio", "spurs", "champianship",
                 "cleveland_cavaliers", "penalty", "halftime", "marty_blake", "ball_hog",
                 "rebound", "chicago_bulls", "philadelphia_76ers", "memphis_grizzlies", "bob_ryan",
                 "half", "wilt_chamberlain", "sam_goldaper", "sacramento_kings", "jerry_west",
                 "charlotte_hornets", "toronto_raptors", "utah_jazz", "quarter", "3point",
                 "harvey_pollack", "derek_fisher", "dave_bing", "red_holzman", "los_angeles_lakers",
                 "fran_blinebury", "blocks", "eurocup", "double_foul", "boston_celtics",
                 "oscar_robertson", "phoenix_suns", "dunk", "jack_ramsay", "shaquille_oneal",
                 "lester_harrison", "denver_nuggets", "defensive", "portland_trail_blazers", "mvp",
                 "new_york_knicks", "magic_johnson", "gene_shue", "chick_hearn", "new_orleans_pelicans",
                 "john_havlicek", "wayne_embry", "washington_wizards", "wes_unseld", "elgin_baylor",
                 "minnesota_timberwolves", "indiana_pacers", "frank_layden", "2point", "marv_albert",
                 "bill_bradley", "shooting_gaurd", "mitch_chortkoff", "peter_vecsey", "hall_of_fame",
                 "power_forward", "atlanta_hawks", "willis_reed", "joe_gilmartin", "overtime",
                 "miami_heat", "dick_mcguire", "isiah_thomas", "fiba", "bob_lanier",
                 "most_valuable_player", "acb", "bill_russell", "red_auerbach", "gloals",
                 "george_mikan", "point_gaurd", "leonard_lewin", "milwaukee_bucks",
                 "kobe_bryant", "leonard_koppett", "elbow", "minutes", "los_angeles_clippers",
                 "bob_pettit", "euroleague", "bob_cousy", "phil_jasner", "dallas_mavericks",
                 "offensive", "dolph_schayes", "larry_bird", "bill_sharman", "david_dupree",
                 "international_basketball_federation", "tim_duncan", "julius_erving", "double_double",
                 "golden_state_warriors",
                 "assist", "free_throw", "hubie_brown", "brooklyn_nets", "houston_rockets",
                 "steals", "turnovers", "kareem_abdul-jabbar", "center", "jack_mccallum",
                 "small_forwards", "oklahoma_city_thunder", "points", "chuck_daly", "michael_jordan"]

NEGATIVE_TERMS = ["football", "golf", "buy", "baseball", "hockey", "facebook", "twitter", "instagram", "pinterest"
                  "money", "makemoney", "pronunciation respelling", "chat", "license", "terms_of_use", "privacy_policy", "pdf"]

# REJECTED_URL_CACHE = {}

def get_score(url, anchor, title, inlink_count, level):
    relevance = 0
    for x in POSITIVE_TERMS:
        if x in url or x in anchor or x in title:
            relevance += 1

    for x in NEGATIVE_TERMS:
        if x in url or x in anchor or x in title:
            relevance = 0

    score = 0.5 * (1/float(level)) + 0.25 * (1- (1/(1+float(inlink_count)))) + 0.25 * (1 - (1/(1+float(relevance))))
    return -score


def reject_url(url):
    reject = False
    #
    # if url in REJECTED_URL_CACHE:
    #     reject = True
    # else:
    for x in NEGATIVE_TERMS:
        if x in url:
            # REJECTED_URL_CACHE.update({url: ''})
            reject = True
            break

    return reject


