/**
 * Copyright 2012 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.mapred.pipelineV3.crawllistgen;

import org.commoncrawl.util.FPGenerator;

import com.google.common.collect.ImmutableSet;

/**
 * 
 * @author rana
 *
 */
public class BlockedDomainList {

  static long blogger_com = FPGenerator.std64.fp("blogger.com");
  static long cgi_ebay_com = FPGenerator.std64.fp("cgi.ebay.com");
  static long twitter_com = FPGenerator.std64.fp("twitter.com");
  static long facebook_com = FPGenerator.std64.fp("facebook.com");
  static long digg_com = FPGenerator.std64.fp("digg.com");
  static long rover_ebay_com = FPGenerator.std64.fp("rover.ebay.com");
  static long addtoany_com = FPGenerator.std64.fp("addtoany.com");
  static long google_com = FPGenerator.std64.fp("google.com");
  static long comments_deviantart_com = FPGenerator.std64.fp("comments.deviantart.com");
  static long shop_ebay_com = FPGenerator.std64.fp("shop.ebay.com");
  static long hotjobs_yahoo_com = FPGenerator.std64.fp("hotjobs.yahoo.com");
  static long apartmentguide_com = FPGenerator.std64.fp("apartmentguide.com");
  static long login_live_com = FPGenerator.std64.fp("login.live.com");
  static long feeds_wordpress_com = FPGenerator.std64.fp("feeds.wordpress.com");
  static long gravatar_com = FPGenerator.std64.fp("gravatar.com");
  static long livejournal_com = FPGenerator.std64.fp("livejournal.com");
  static long myspace_com = FPGenerator.std64.fp("myspace.com");
  static long adlog_com_com = FPGenerator.std64.fp("adlog.com.com");
  static long clothing_shop_ebay_com = FPGenerator.std64.fp("clothing.shop.ebay.com");
  static long maps_google_com = FPGenerator.std64.fp("maps.google.com");
  static long local_yahoo_com = FPGenerator.std64.fp("local.yahoo.com");
  static long feedback_ebay_com = FPGenerator.std64.fp("feedback.ebay.com");
  static long books_shop_ebay_com = FPGenerator.std64.fp("books.shop.ebay.com");
  static long ep_yimg_com = FPGenerator.std64.fp("ep.yimg.com");
  static long collectibles_shop_ebay_com = FPGenerator.std64.fp("collectibles.shop.ebay.com");
  static long astore_amazon_com = FPGenerator.std64.fp("astore.amazon.com");
  static long calendar_yahoo_com = FPGenerator.std64.fp("calendar.yahoo.com");
  static long compare_buscape_com_br = FPGenerator.std64.fp("compare.buscape.com.br");
  static long cafepress_com = FPGenerator.std64.fp("cafepress.com");
  static long simpy_com = FPGenerator.std64.fp("simpy.com");
  static long catalog_ebay_com = FPGenerator.std64.fp("catalog.ebay.com");
  static long wikimapia_org = FPGenerator.std64.fp("wikimapia.org");
  static long yp_yahoo_com = FPGenerator.std64.fp("yp.yahoo.com");
  static long translate_google_com = FPGenerator.std64.fp("translate.google.com");
  static long dogpile_com = FPGenerator.std64.fp("dogpile.com");
  static long myworld_ebay_com = FPGenerator.std64.fp("myworld.ebay.com");
  static long rockauto_com = FPGenerator.std64.fp("rockauto.com");
  static long friendster_com = FPGenerator.std64.fp("friendster.com");
  static long diigo_com = FPGenerator.std64.fp("diigo.com");
  static long yellowpages_com = FPGenerator.std64.fp("yellowpages.com");
  static long stat_dealtime_com = FPGenerator.std64.fp("stat.dealtime.com");
  static long openads_mediouno_com = FPGenerator.std64.fp("openads.mediouno.com");
  static long nextag_com = FPGenerator.std64.fp("nextag.com");
  static long trulia_com = FPGenerator.std64.fp("trulia.com");
  static long ask_reference_com = FPGenerator.std64.fp("ask.reference.com");
  static long aboutus_org = FPGenerator.std64.fp("aboutus.org");
  static long opensubtitles_org = FPGenerator.std64.fp("opensubtitles.org");
  static long furl_net = FPGenerator.std64.fp("furl.net");
  static long bebo_com = FPGenerator.std64.fp("bebo.com");
  static long one_gravatar_com = FPGenerator.std64.fp("1.gravatar.com");
  static long two_gravatar_com = FPGenerator.std64.fp("0.gravatar.com");
  static long sedoparking_com = FPGenerator.std64.fp("sedoparking.com");
  static long dvd_shop_ebay_com = FPGenerator.std64.fp("dvd.shop.ebay.com");
  static long news_google_com = FPGenerator.std64.fp("news.google.com");
  static long upmystreet_com = FPGenerator.std64.fp("upmystreet.com");
  static long music_shop_ebay_com = FPGenerator.std64.fp("music.shop.ebay.com");
  static long alibris_com = FPGenerator.std64.fp("alibris.com");
  static long res99_com = FPGenerator.std64.fp("res99.com");
  static long blogcatalog_com = FPGenerator.std64.fp("blogcatalog.com");
  static long discogs_com = FPGenerator.std64.fp("discogs.com");
  static long boardreader_com = FPGenerator.std64.fp("boardreader.com");
  static long open_thumbshots_org = FPGenerator.std64.fp("open.thumbshots.org");
  static long toys_shop_ebay_com = FPGenerator.std64.fp("toys.shop.ebay.com");
  static long myweb2_search_yahoo_com = FPGenerator.std64.fp("myweb2.search.yahoo.com");
  static long api_tweetmeme_com = FPGenerator.std64.fp("api.tweetmeme.com");
  static long ads_fulldls_com = FPGenerator.std64.fp("ads.fulldls.com");
  static long us_rd_yahoo_com = FPGenerator.std64.fp("us.rd.yahoo.com");
  static long newsvine_com = FPGenerator.std64.fp("newsvine.com");
  static long computers_shop_ebay_com = FPGenerator.std64.fp("computers.shop.ebay.com");
  static long login_yahoo_com = FPGenerator.std64.fp("login.yahoo.com");
  static long sports_cards_shop_ebay_com = FPGenerator.std64.fp("sports-cards.shop.ebay.com");
  static long mylife_com = FPGenerator.std64.fp("mylife.com");
  static long spaces_live_com = FPGenerator.std64.fp("spaces.live.com");
  static long cgi1_ebay_com = FPGenerator.std64.fp("cgi1.ebay.com");
  static long topix_com = FPGenerator.std64.fp("topix.com");
  static long profile_live_com = FPGenerator.std64.fp("profile.live.com");
  static long iacas_adbureau_net = FPGenerator.std64.fp("iacas.adbureau.net");
  static long babylon_com = FPGenerator.std64.fp("babylon.com");
  static long jewelry_shop_ebay_com = FPGenerator.std64.fp("jewelry.shop.ebay.com");
  static long laterooms_com = FPGenerator.std64.fp("laterooms.com");
  static long allbreedpedigree_com = FPGenerator.std64.fp("allbreedpedigree.com");
  static long b_scorecardresearch_com = FPGenerator.std64.fp("b.scorecardresearch.com");
  static long allpoetry_com = FPGenerator.std64.fp("allpoetry.com");
  static long product_half_ebay_com = FPGenerator.std64.fp("product.half.ebay.com");
  static long highbeam_com = FPGenerator.std64.fp("highbeam.com");
  static long hi5_com = FPGenerator.std64.fp("hi5.com");
  static long ufindus_com = FPGenerator.std64.fp("ufindus.com");
  static long d1_openx_org = FPGenerator.std64.fp("d1.openx.org");
  static long video_aol_com = FPGenerator.std64.fp("video.aol.com");
  static long robtex_com = FPGenerator.std64.fp("robtex.com");
  static long dw_com_com = FPGenerator.std64.fp("dw.com.com");
  static long dmoz_org = FPGenerator.std64.fp("dmoz.org");
  static long citeulike_org = FPGenerator.std64.fp("citeulike.org");
  static long findarticles_com = FPGenerator.std64.fp("findarticles.com");
  static long public_bay_livefilestore_com = FPGenerator.std64.fp("public.bay.livefilestore.com");
  static long badoo_com = FPGenerator.std64.fp("badoo.com");
  static long bloglines_com = FPGenerator.std64.fp("bloglines.com");
  static long articlesbase_com = FPGenerator.std64.fp("articlesbase.com");
  static long viewmorepics_myspace_com = FPGenerator.std64.fp("viewmorepics.myspace.com");
  static long signup_alerts_live_com = FPGenerator.std64.fp("signup.alerts.live.com");
  static long search_ancestry_com = FPGenerator.std64.fp("search.ancestry.com");
  static long nfodb_com = FPGenerator.std64.fp("nfodb.com");
  static long propeller_com = FPGenerator.std64.fp("propeller.com");
  static long mybloglog_com = FPGenerator.std64.fp("mybloglog.com");
  static long mapquest_com = FPGenerator.std64.fp("mapquest.com");
  static long imeem_com = FPGenerator.std64.fp("imeem.com");
  static long fulldls_com = FPGenerator.std64.fp("fulldls.com");
  static long scholar_google_com = FPGenerator.std64.fp("scholar.google.com");
  static long search_com = FPGenerator.std64.fp("search.com");
  static long profiles_yahoo_com = FPGenerator.std64.fp("profiles.yahoo.com");
  static long rapidshare_com = FPGenerator.std64.fp("rapidshare.com");
  static long snap9_advertserve_com = FPGenerator.std64.fp("snap9.advertserve.com");
  static long edit_yahoo_com = FPGenerator.std64.fp("edit.yahoo.com");
  static long blinklist_com = FPGenerator.std64.fp("blinklist.com");
  static long depositfiles_com = FPGenerator.std64.fp("depositfiles.com");
  static long truveo_com = FPGenerator.std64.fp("truveo.com");
  static long shoebuy_com = FPGenerator.std64.fp("shoebuy.com");
  static long cgi6_ebay_com = FPGenerator.std64.fp("cgi6.ebay.com");
  static long img1_mlstatic_com = FPGenerator.std64.fp("img1.mlstatic.com");
  static long m_youtube_com = FPGenerator.std64.fp("m.youtube.com");
  static long cgi3_ebay_com = FPGenerator.std64.fp("cgi3.ebay.com");
  static long geo_yahoo_com = FPGenerator.std64.fp("geo.yahoo.com");
  static long mytravelguide_com = FPGenerator.std64.fp("mytravelguide.com");
  static long item_shopping_c_yimg_jp = FPGenerator.std64.fp("item.shopping.c.yimg.jp");
  static long mediaranking_net = FPGenerator.std64.fp("mediaranking.net");
  static long warezaccess_com = FPGenerator.std64.fp("warezaccess.com");
  static long zde_am_affinity_com = FPGenerator.std64.fp("zde.am.affinity.com");
  static long online_sagepub_com = FPGenerator.std64.fp("online.sagepub.com");
  static long kqzyfj_com = FPGenerator.std64.fp("kqzyfj.com");
  static long widgetbox_com = FPGenerator.std64.fp("widgetbox.com");
  static long altfarm_mediaplex_com = FPGenerator.std64.fp("altfarm.mediaplex.com");

  static ImmutableSet<Long> blockedDomains = new ImmutableSet.Builder<Long>()


    .add(blogger_com)
    .add(cgi_ebay_com)
    .add(twitter_com)
    .add(facebook_com)
    .add(rover_ebay_com)
    .add(addtoany_com)
    .add(google_com)
    .add(comments_deviantart_com)
    .add(shop_ebay_com)
    .add(apartmentguide_com)
    .add(login_live_com)
    .add(feeds_wordpress_com)
    .add(gravatar_com)
    .add(adlog_com_com)
    .add(clothing_shop_ebay_com)
    .add(maps_google_com)
    .add(feedback_ebay_com)
    .add(books_shop_ebay_com)
    .add(ep_yimg_com)
    .add(collectibles_shop_ebay_com)
    .add(astore_amazon_com)
    .add(calendar_yahoo_com)
    .add(compare_buscape_com_br)
    .add(cafepress_com)
    .add(simpy_com)
    .add(catalog_ebay_com)
    .add(wikimapia_org)
    .add(translate_google_com)
    .add(dogpile_com)
    .add(myworld_ebay_com)
    .add(rockauto_com)
    .add(friendster_com)
    .add(diigo_com)
    .add(stat_dealtime_com)
    .add(openads_mediouno_com)
    .add(nextag_com)
    .add(trulia_com)
    .add(ask_reference_com)
    .add(aboutus_org)
    .add(opensubtitles_org)
    .add(furl_net)
    .add(bebo_com)
    .add(one_gravatar_com)
    .add(two_gravatar_com)
    .add(sedoparking_com)
    .add(dvd_shop_ebay_com)
    .add(upmystreet_com)
    .add(music_shop_ebay_com)
    .add(alibris_com)
    .add(res99_com)
    .add(blogcatalog_com)
    .add(discogs_com)
    .add(boardreader_com)
    .add(open_thumbshots_org)
    .add(toys_shop_ebay_com)
    .add(myweb2_search_yahoo_com)
    .add(api_tweetmeme_com)
    .add(ads_fulldls_com)
    .add(us_rd_yahoo_com)
    .add(newsvine_com)
    .add(computers_shop_ebay_com)
    
    .add(login_yahoo_com)
    .add(sports_cards_shop_ebay_com)
    .add(mylife_com)
    .add(spaces_live_com)
    .add(cgi1_ebay_com)
    
    .add(topix_com)
    .add(profile_live_com)
    .add(iacas_adbureau_net)
    .add(babylon_com)
    .add(jewelry_shop_ebay_com)
    .add(laterooms_com)
    .add(allbreedpedigree_com)
    .add(b_scorecardresearch_com)
    .add(allpoetry_com)
    .add(product_half_ebay_com)
    .add(highbeam_com)
    .add(hi5_com)
    .add(ufindus_com)
    .add(d1_openx_org)
    .add(video_aol_com)
    .add(robtex_com)
    .add(dw_com_com)
    .add(dmoz_org)
    .add(citeulike_org)
    .add(findarticles_com)
    .add(public_bay_livefilestore_com)
    .add(badoo_com)
    .add(articlesbase_com)
    .add(viewmorepics_myspace_com)
    .add(signup_alerts_live_com)
    .add(search_ancestry_com)
    .add(nfodb_com)
    .add(propeller_com)
    .add(mybloglog_com)
    .add(mapquest_com)
    .add(imeem_com)
    .add(fulldls_com)
    .add(scholar_google_com)
    .add(search_com)
    .add(profiles_yahoo_com)
    .add(rapidshare_com)
    .add(snap9_advertserve_com)
    .add(edit_yahoo_com)
    .add(blinklist_com)
    .add(depositfiles_com)
    .add(shoebuy_com)
    .add(cgi6_ebay_com)
    .add(img1_mlstatic_com)
    .add(m_youtube_com)
    .add(cgi3_ebay_com)
    .add(geo_yahoo_com)
    .add(mytravelguide_com)
    .add(item_shopping_c_yimg_jp)
    .add(mediaranking_net)
    .add(warezaccess_com)
    .add(zde_am_affinity_com)
    .add(online_sagepub_com)
    .add(kqzyfj_com)
    .add(widgetbox_com)
    .add(altfarm_mediaplex_com)


    .build();
}
