with
    source_data as (
        select
            sp.ts,
            device_id,
            event_id,
            nvl(lag(event_id, 1) over (partition by device_id order by sent_ts, event_name), ' ') as prev_event_id,
            nvl(
                datediff(minute, (lag(sent_ts, 1) over (partition by device_id order by sent_ts, event_name)), sent_ts),
                1000
            ) as last_time,
            nvl(
                datediff(
                    minute, sent_ts, (lead(sent_ts, 1) over (partition by device_id order by sent_ts, event_name))
                ),
                1000
            ) as next_time,
            rank() over (partition by device_id, event_name order by sent_ts) as event_seq,
            user_id,
            country,
            region,
            city,
            dma,
            timezone,
            model,

    case when platform = 'APPLETV' then 'TVOS' else platform end
 as platform,

    case
        when
            upper(platform) in (
                'IPHONE',
                'IPAD',
                'ANDROID',
                'FIRETABLET',
                'ANDROID-SAMSUNG',
                'ANDROID_SAMSUNG',
                'FOR_SAMSUNG',
                'IOS_WEB',
                'IOS'
            )
        then 'MOBILE'
        when upper(platform) in ('WEB')
        then 'WEB'
        else 'OTT'
    end
 as platform_type,
            app_version,
            content_id,
            case when content_type = 'EPISODE' then content_series_id else content_id end as program_id,
            -- most analysis done at SERIES level, not EPISODE. use `is_episode`
            -- column in
            -- downstream EPISODE related tables
            case when content_type = 'EPISODE' then 'SERIES' else content_type end as _content_type,
            case when content_type = 'EPISODE' then true else false end as is_episode,
            title as content_name,
            container_type,
            page_type,
            dest_page_type,
            campaign,
            source,
            os,
            os_version,
            manufacturer,
            advertiser_id,
            is_mobile,
            app_mode,
            event_name,
            notification_status,

    case
        when
            platform in ('IPAD', 'IPHONE')
            and app_version >= '4.6.0'
            and app_version <= '4.8.2'
            and view_time >= position
        then datediff(millisecond, lag(sent_ts, 1) over (partition by device_id order by sent_ts, event_name), sent_ts)
        else case when event_name = 'PlayProgressEvent' then greatest(view_time, 0) else 0 end
    end
 as clean_view_time,
            case
                when nvl(from_autoplay_deliberate, false) or nvl(from_autoplay_automatic, false) then true else false
            end as is_autoplay,
            case when event_name = 'LivePlayProgressEvent' then greatest(view_time, 0) else 0 end as linear_view_time,
            case when event_name = 'ScenesPlayProgressEvent' then greatest(view_time, 0) else 0 end as scenes_view_time,
            case
                when event_name = 'PreviewPlayProgressEvent' then greatest(view_time, 0) else 0
            end as preview_view_time,
            from_autoplay_deliberate,
            from_autoplay_automatic,
            device_first_seen_ts,
            device_first_view_ts,
            user_first_seen_ts,
            user_first_view_ts,
            position / 1000::numeric as position_sec,
            duration,
            manip,
            status,
            auth_type,
            device_language,
            op,
            content_mode
        from `hive_metastore`.`tubidw_dev`.`analytics_richevent` as sp
        where
            event_name in (
        'PlayProgressEvent',
        'StartVideoEvent',
        'StartTrailerEvent',
        'PageLoadEvent',
        'AccountEvent',
        'ActiveEvent',
        'StartAdEvent',
        'FinishAdEvent',
        'SubtitlesToggleEvent',
        'SearchEvent',
        'SeekEvent',
        'ResumeAfterBreakEvent',
        'PauseToggleEvent',
        'CastEvent',
        'LivePlayProgressEvent',
        'LivePlayProgressEventEvent',
        'StartLiveVideoEvent',
        'BookmarkEvent',
        'NavigateToPageEvent',
        'PreviewPlayProgressEvent',
        'ScenesPlayProgressEvent',
        'StartScenesEvent'
    )
            and
   user_agent NOT IN (
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0',
                       'runscope-radar/2.0',
                       'Mozilla/5.0 (X11; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0',
                       'Mozilla/5.0 (Windows NT 6.3;compatible; Leikibot/1.0; +http://www.leiki.com)'
    )

  AND platform||app_version != 'AMAZON2.16.5'
            and
      TO_DATE(date, "yyyy-MM-dd") >= DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd"))
        AND TO_DATE(date, "yyyy-MM-dd") < DATE_TRUNC("day", TO_DATE('2023-08-01', "yyyy-MM-dd"))
            and upper(video_player) != 'EXTERNAL_PREVIEW'
            and not nvl(is_robot, false)
    ),
    enriched_data as (
        select
            *,
            case
                when _content_type = 'MOVIE' and is_autoplay then nvl(clean_view_time, 0) else 0
            end as movie_autoplay_view_time,
            case
                when _content_type = 'SERIES' and is_autoplay then nvl(clean_view_time, 0) else 0
            end as series_autoplay_view_time,
            case
                when _content_type = 'SPORTS_EVENT' and is_autoplay then nvl(clean_view_time, 0) else 0
            end as sports_event_autoplay_view_time,
            case
                when _content_type = 'MOVIE' and is_autoplay = false then nvl(clean_view_time, 0) else 0
            end as movie_non_autoplay_view_time,
            case
                when _content_type = 'SERIES' and is_autoplay = false then nvl(clean_view_time, 0) else 0
            end as series_non_autoplay_view_time,
            case
                when _content_type = 'SPORTS_EVENT' and is_autoplay = false then nvl(clean_view_time, 0) else 0
            end as sports_event_non_autoplay_view_time,
            case
                when _content_type not in ('SERIES', 'MOVIE', 'SPORTS_EVENT', 'SCENE') and is_autoplay = false
                then nvl(clean_view_time, 0)
                else 0
            end as unattributed_non_autoplay_view_time,
            case
                when _content_type not in ('SERIES', 'MOVIE', 'SPORTS_EVENT', 'SCENE') and is_autoplay
                then nvl(clean_view_time, 0)
                else 0
            end as unattributed_autoplay_view_time
        from source_data
    ),
    all_data_1 as (
        select
            md5(cast(concat(coalesce(cast(DATE_TRUNC('hour', ts) as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(device_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(user_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(region as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(dma as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(timezone as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(model as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(platform as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(platform_type as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(app_version as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(program_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(content_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(_content_type as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(DECODE(is_episode, TRUE, 'T', FALSE, 'F', NULL) as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(content_name as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(container_type as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(page_type as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(campaign as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(source as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(os as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(os_version as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(manufacturer as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(advertiser_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(DECODE(is_mobile, TRUE, 'T', FALSE, 'F', NULL) as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(app_mode as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(auth_type as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(device_language as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(notification_status as string), '_dbt_utils_surrogate_key_null_')) as string)) as _id,
            max(d.ts) as _last_updated,
            date_trunc('hour', d.ts) as hs,
            device_id,
            user_id,
            nvl(user_id, device_id) as user_alias,
            min(device_first_seen_ts) as device_first_seen_ts,
            min(device_first_view_ts) as device_first_view_ts,
            min(nvl(user_first_seen_ts, device_first_seen_ts)) as user_first_seen_ts,
            min(nvl(user_first_view_ts, device_first_view_ts)) as user_first_view_ts,
            country,
            region,
            city,
            dma,
            timezone,
            model,
            platform,
            platform_type,
            app_version,
            content_id,
            program_id,
            is_episode,
            _content_type,
            content_name,
            container_type,
            page_type,
            campaign,
            source,
            os,
            os_version,
            manufacturer,
            advertiser_id,
            is_mobile,
            app_mode,
            auth_type,
            device_language,
            notification_status,
            sum(case when last_time >= 30 then 1 else 0 end) as visit_total_count,
            sum(movie_autoplay_view_time) as movie_autoplay_tvt,
            sum(movie_non_autoplay_view_time) as movie_non_autoplay_tvt,
            sum(series_autoplay_view_time) as series_autoplay_tvt,
            sum(series_non_autoplay_view_time) as series_non_autoplay_tvt,
            sum(sports_event_autoplay_view_time) as sports_event_autoplay_tvt,
            sum(sports_event_non_autoplay_view_time) as sports_event_non_autoplay_tvt,
            sum(unattributed_non_autoplay_view_time) / 1000::numeric as unattributed_non_autoplay_tvt_sec,
            sum(unattributed_autoplay_view_time) / 1000::numeric as unattributed_autoplay_tvt_sec,
            least(sum(linear_view_time / 1000::numeric), 3600) as linear_tvt_sec,
            least(sum(scenes_view_time / 1000::numeric), 3600) as scenes_tvt_sec,
            sum(preview_view_time / 1000::numeric) as preview_tvt_sec,
            sum(case when clean_view_time >= 3600000 then 1 else 0 end) as tvt_cap_total_count,
            sum(case when event_name = 'StartVideoEvent' then 1 else 0 end) as view_total_count,
            sum(
                case when event_name = 'StartScenesEvent' and _content_type = 'SCENE' then 1 else 0 end
            ) as scenes_view_total_count,
            sum(
                case when event_name = 'StartVideoEvent' and _content_type = 'SERIES' then 1 else 0 end
            ) as series_view_total_count,
            sum(
                case when event_name = 'StartVideoEvent' and _content_type = 'MOVIE' then 1 else 0 end
            ) as movie_view_total_count,
            sum(
                case when event_name = 'StartVideoEvent' and _content_type = 'SPORTS_EVENT' then 1 else 0 end
            ) as sports_event_view_total_count,
            sum(
                case
                    when
                        event_name = 'StartVideoEvent'
                        and (nvl(from_autoplay_deliberate, false) or nvl(from_autoplay_automatic, false))
                    then 1
                    else 0
                end
            ) as autoplay_view_total_count,
            sum(
                case
                    when
                        event_name = 'StartVideoEvent'
                        and nvl(from_autoplay_deliberate, false) = false
                        and nvl(from_autoplay_automatic, false) = false
                    then 1
                    else 0
                end
            ) as non_autoplay_view_total_count,
            sum(
                case
                    when
                        event_name = 'StartVideoEvent'
                        and _content_type = 'MOVIE'
                        and (nvl(from_autoplay_deliberate, false) or nvl(from_autoplay_automatic, false))
                    then 1
                    else 0
                end
            ) as autoplay_movie_view_total_count,
            sum(
                case
                    when
                        event_name = 'StartVideoEvent'
                        and _content_type = 'SERIES'
                        and (nvl(from_autoplay_deliberate, false) or nvl(from_autoplay_automatic, false))
                    then 1
                    else 0
                end
            ) as autoplay_series_view_total_count,
            sum(
                case
                    when
                        event_name = 'StartVideoEvent'
                        and _content_type = 'SPORTS_EVENT'
                        and (nvl(from_autoplay_deliberate, false) or nvl(from_autoplay_automatic, false))
                    then 1
                    else 0
                end
            ) as autoplay_sports_event_view_total_count,
            sum(case when event_name = 'SeekEvent' then 1 else 0 end) as seek_total_count,
            sum(case when event_name = 'PauseToggleEvent' then 1 else 0 end) as pause_total_count,
            sum(case when event_name = 'SubtitlesToggleEvent' then 1 else 0 end) as subtitles_total_count,
            sum(case when event_name = 'SearchEvent' then 1 else 0 end) as search_total_count,
            sum(
                case
                    when
                        event_name = 'NavigateToPageEvent'
                        and page_type = 'SearchPage'
                        and dest_page_type in ('VideoPage', 'SeriesDetailPage')
                    then 1
                    else 0
                end
            ) as search_click_through_count,
            sum(
                case
                    when
                        event_name = 'NavigateToPageEvent'
                        and page_type = 'VideoPlayerPage'
                        and dest_page_type in ('VideoPage', 'SeriesDetailPage')
                    then 1
                    else 0
                end
            ) as player_exit_count,
            sum(
                case when event_name = 'BookmarkEvent' and op = 'ADD_TO_QUEUE' then 1 else 0 end
            ) as add_to_queue_total_count,
            sum(
                case when event_name = 'BookmarkEvent' and op = 'REMOVE_FROM_QUEUE' then 1 else 0 end
            ) as remove_from_queue_total_count,
            sum(
                case when event_name = 'BookmarkEvent' and op = 'REMOVE_FROM_CONTINUE_WATCHING' then 1 else 0 end
            ) as remove_from_cw_total_count,
            sum(
                case
                    when event_name = 'PageLoadEvent' and page_type in ('VideoPage', 'SeriesDetailPage') then 1 else 0
                end
            ) as details_page_visit_total_count,
            sum(
                case
                    when
                        event_name = 'PageLoadEvent'
                        and page_type = 'OnboardingPage'
                        and dest_page_type = 'OnboardingPage'
                    then 1
                    else 0
                end
            ) as onboarding_page_visit_total_count,
            sum(
                case when event_name = 'PageLoadEvent' and page_type = 'HomePage' then 1 else 0 end
            ) as home_page_visit_total_count,
            sum(
                case when event_name = 'PageLoadEvent' and page_type = 'BrowsePage' then 1 else 0 end
            ) as browse_page_visit_total_count,
            sum(
                case when event_name = 'PageLoadEvent' and page_type = 'CategoryPage' then 1 else 0 end
            ) as category_page_visit_total_count,
            sum(case when event_name = 'StartAdEvent' then 1 else 0 end) as ad_impression_total_count,
            sum(case when event_name = 'ResumeAfterBreakEvent' then 1 else 0 end) as ad_break_total_count,
            sum(case when event_name = 'StartTrailerEvent' then 1 else 0 end) as trailer_start_count,
            max(
                case
                    when
                        event_name = 'AccountEvent'
                        and manip = 'SIGNUP'
                        and (status = 'SUCCESS' or status = 'UNKNOWN_ACTION_STATUS')
                    then 1
                    else 0
                end
            ) as user_signup_count,
            max(
                case
                    when
                        event_name = 'AccountEvent'
                        and manip = 'REGISTER_DEVICE'
                        and (status = 'SUCCESS' or status = 'UNKNOWN_ACTION_STATUS')
                    then 1
                    else 0
                end
            ) as device_registration_count,
            sum(case when event_name = 'CastEvent' then 1 else 0 end) as cast_count,
            sum(
                case when event_name = 'NavigateToPageEvent' and dest_page_type = 'ForYouPage' then 1 else 0 end
            ) as for_you_page_visit_total_count,
            sum(
                case
                    when
                        event_name = 'PageLoadEvent'
                        and page_type = 'HomePage'
                        and content_mode = 'CONTENT_MODE_LATINO'
                        and status = 'SUCCESS'
                    then 1
                    else 0
                end
            ) as latino_browse_page_visit_total_count,
            sum(
                case
                    when event_name = 'PageLoadEvent' and page_type = 'HomePage' and app_mode = 'KIDS_MODE'
                    then 1
                    else 0
                end
            ) as kids_browse_page_visit_total_count,
            sum(
                case when event_name = 'NavigateToPageEvent' and dest_page_type = 'LinearBrowsePage' then 1 else 0 end
            ) as linear_browse_page_visit_total_count,
            sum(
                case
                    when
                        event_name = 'NavigateToPageEvent'
                        and dest_page_type = 'HomePage'
                        and content_mode = 'CONTENT_MODE_MOVIE'
                    then 1
                    else 0
                end
            ) as movie_browse_page_visit_total_count,
            sum(
                case
                    when
                        event_name = 'NavigateToPageEvent'
                        and dest_page_type = 'HomePage'
                        and content_mode = 'CONTENT_MODE_TV'
                    then 1
                    else 0
                end
            ) as series_browse_page_visit_total_count,
            sum(
                case
                    when
                        event_name = 'PageLoadEvent'
                        and page_type = 'HomePage'
                        and content_mode = 'CONTENT_MODE_UNKNOWN'
                        and status = 'SUCCESS'
                    then 1
                    else 0
                end
            ) as home_page_no_modes_visit_total_count,
            sum(
                case when event_name = 'NavigateToPageEvent' and dest_page_type = 'SearchPage' then 1 else 0 end
            ) as search_page_visit_total_count,
            sum(
                case
                    when event_name = 'PageLoadEvent' and page_type = 'ForYouPage' and auth_type = 'NOT_AUTHED'
                    then 1
                    else 0
                end
            ) as for_you_page_registration_gate_total_count,
            case
                when
                    round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 5
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 5
                then 1
                else 0
            end as complete_5p_total_count,
            case
                when
                    round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 30
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 30
                then 1
                else 0
            end as complete_30p_total_count,
            case
                when
                    round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 70
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 70
                then 1
                else 0
            end as complete_70p_total_count,
            case
                when
                    round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 90
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 90
                then 1
                else 0
            end as complete_90p_total_count,
            case
                when
                    _content_type = 'MOVIE'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 30
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 30
                then 1
                else 0
            end as movie_complete_30p_total_count,
            case
                when
                    _content_type = 'MOVIE'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 70
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 70
                then 1
                else 0
            end as movie_complete_70p_total_count,
            case
                when
                    _content_type = 'MOVIE'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 90
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 90
                then 1
                else 0
            end as movie_complete_90p_total_count,
            case
                when
                    _content_type = 'SERIES'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 30
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 30
                then 1
                else 0
            end as episode_complete_30p_total_count,
            case
                when
                    _content_type = 'SERIES'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 70
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 70
                then 1
                else 0
            end as episode_complete_70p_total_count,
            case
                when
                    _content_type = 'SERIES'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 90
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 90
                then 1
                else 0
            end as episode_complete_90p_total_count,
            case
                when
                    _content_type = 'SPORTS_EVENT'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 30
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 30
                then 1
                else 0
            end as sports_event_complete_30p_total_count,
            case
                when
                    _content_type = 'SPORTS_EVENT'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 70
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 70
                then 1
                else 0
            end as sports_event_complete_70p_total_count,
            case
                when
                    _content_type = 'SPORTS_EVENT'
                    and round((min(position_sec)::numeric / max(duration)) * 100, 0) <= 90
                    and round((max(position_sec)::numeric / max(duration)) * 100, 0) >= 90
                then 1
                else 0
            end as sports_event_complete_90p_total_count,
            sum(case when event_name = 'StartLiveVideoEvent' then 1 else 0 end) as linear_view_total_count
        from enriched_data as d

        where (last_time <= 30 or next_time <= 30) and event_id != prev_event_id
        group by _id, hs, device_id, user_id, user_alias, country, region, city, dma, timezone, model, platform, platform_type, app_version, content_id, program_id, is_episode, _content_type, content_name, container_type, page_type, campaign, source, os, os_version, manufacturer, advertiser_id, is_mobile, app_mode, auth_type, device_language, notification_status
    ),
    all_data_2 as (
        select
            *,
            case
                when series_autoplay_tvt >= 3600000 and series_non_autoplay_tvt >= 3600000
                then 1800
                when series_autoplay_tvt >= 3600000 and series_non_autoplay_tvt < 3600000
                then 3600 - (series_non_autoplay_tvt / 1000::numeric)
                else series_autoplay_tvt / 1000::numeric
            end as series_autoplay_tvt_sec,
            case
                when series_autoplay_tvt >= 3600000 and series_non_autoplay_tvt >= 3600000
                then 1800
                when series_autoplay_tvt < 3600000 and series_non_autoplay_tvt >= 3600000
                then 3600 - (series_autoplay_tvt / 1000::numeric)
                else (series_non_autoplay_tvt / 1000::numeric)
            end as series_non_autoplay_tvt_sec,
            case
                when movie_autoplay_tvt >= 3600000 and movie_non_autoplay_tvt >= 3600000
                then 1800
                when movie_autoplay_tvt >= 3600000 and movie_non_autoplay_tvt < 3600000
                then 3600 - (movie_non_autoplay_tvt / 1000::numeric)
                else movie_autoplay_tvt / 1000::numeric
            end as movie_autoplay_tvt_sec,
            case
                when movie_autoplay_tvt >= 3600000 and movie_non_autoplay_tvt >= 3600000
                then 1800
                when movie_autoplay_tvt < 3600000 and movie_non_autoplay_tvt >= 3600000
                then 3600 - (movie_autoplay_tvt / 1000::numeric)
                else (movie_non_autoplay_tvt / 1000::numeric)
            end as movie_non_autoplay_tvt_sec,
            case
                when sports_event_autoplay_tvt >= 3600000 and sports_event_non_autoplay_tvt >= 3600000
                then 1800
                when sports_event_autoplay_tvt >= 3600000 and sports_event_non_autoplay_tvt < 3600000
                then 3600 - (sports_event_non_autoplay_tvt / 1000::numeric)
                else sports_event_autoplay_tvt / 1000::numeric
            end as sports_event_autoplay_tvt_sec,
            case
                when sports_event_autoplay_tvt >= 3600000 and sports_event_non_autoplay_tvt >= 3600000
                then 1800
                when sports_event_autoplay_tvt < 3600000 and sports_event_non_autoplay_tvt >= 3600000
                then 3600 - (sports_event_autoplay_tvt / 1000::numeric)
                else (sports_event_non_autoplay_tvt / 1000::numeric)
            end as sports_event_non_autoplay_tvt_sec
        from all_data_1
    ),
    all_data_3 as (
        select
            *,
            series_autoplay_tvt_sec
            + movie_autoplay_tvt_sec
            + sports_event_autoplay_tvt_sec
            + unattributed_autoplay_tvt_sec as autoplay_tvt_sec,
            series_non_autoplay_tvt_sec
            + movie_non_autoplay_tvt_sec
            + sports_event_non_autoplay_tvt_sec
            + unattributed_non_autoplay_tvt_sec as non_autoplay_tvt_sec,
            series_autoplay_tvt_sec + series_non_autoplay_tvt_sec as series_tvt_sec,
            movie_autoplay_tvt_sec + movie_non_autoplay_tvt_sec as movie_tvt_sec,
            sports_event_autoplay_tvt_sec + sports_event_non_autoplay_tvt_sec as sports_event_tvt_sec,
            unattributed_non_autoplay_tvt_sec + unattributed_autoplay_tvt_sec as unattributed_tvt_sec
        from all_data_2
    ),
    all_data_4 as (
        select
            *,
            autoplay_tvt_sec + non_autoplay_tvt_sec as tvt_sec,
            (autoplay_tvt_sec + non_autoplay_tvt_sec) / 60::numeric as tvt_min
        from all_data_3
    ),
    user_data as (
        select
            _id,
            u.user_first_seen_ts,
            u.user_first_view_ts,
            u.user_first_linear_view_ts,
            u.user_first_vod_view_ts,
            u.user_first_platform,
            u.user_first_country
        from all_data_4 as a
        left outer join `hive_metastore`.`tubidw_dev`.`user_first_metrics` as u using (user_id)
        where a.user_id is not null
    )
select
    _id,
    x._last_updated,
    hs,
    date(hs) as ds,
    x.device_id,
    x.user_id,
    user_alias,
    d.device_first_seen_ts as device_first_seen_ts,
    d.device_first_view_ts as device_first_view_ts,
    d.device_first_linear_view_ts as device_first_linear_view_ts,
    d.device_first_vod_view_ts as device_first_vod_view_ts,
    u.user_first_seen_ts as user_first_seen_ts,
    u.user_first_view_ts as user_first_view_ts,
    u.user_first_linear_view_ts as user_first_linear_view_ts,
    u.user_first_vod_view_ts as user_first_vod_view_ts,
    u.user_first_platform,
    u.user_first_country,
    country,
    region,
    city,
    dma,
    timezone,
    model,
    platform,
    platform_type,
    app_version,
    content_id,
    program_id,
    is_episode,
    _content_type as content_type,
    content_name,
    container_type,
    page_type,
    campaign,
    source,
    os,
    os_version,
    manufacturer,
    advertiser_id,
    is_mobile,
    app_mode,
    auth_type,
    device_language,
    notification_status,
    visit_total_count,
    tvt_sec,
    tvt_min,
    movie_tvt_sec,
    scenes_tvt_sec,
    scenes_view_total_count,
    series_tvt_sec,
    sports_event_tvt_sec,
    unattributed_tvt_sec,
    autoplay_tvt_sec,
    non_autoplay_tvt_sec,
    movie_autoplay_tvt_sec,
    series_autoplay_tvt_sec,
    sports_event_autoplay_tvt_sec,
    movie_non_autoplay_tvt_sec,
    series_non_autoplay_tvt_sec,
    sports_event_non_autoplay_tvt_sec,
    null::integer as pause_total_sec,
    linear_tvt_sec,
    preview_tvt_sec,
    tvt_cap_total_count,
    view_total_count,
    series_view_total_count,
    sports_event_view_total_count,
    movie_view_total_count,
    autoplay_view_total_count,
    non_autoplay_view_total_count,
    autoplay_movie_view_total_count,
    autoplay_series_view_total_count,
    autoplay_sports_event_view_total_count,
    seek_total_count,
    pause_total_count,
    subtitles_total_count,
    search_total_count,
    search_click_through_count,
    player_exit_count,
    add_to_queue_total_count,
    remove_from_queue_total_count,
    remove_from_cw_total_count,
    details_page_visit_total_count,
    browse_page_visit_total_count,
    category_page_visit_total_count,
    home_page_visit_total_count,
    onboarding_page_visit_total_count,
    ad_impression_total_count,
    ad_break_total_count,
    trailer_start_count,
    user_signup_count,
    device_registration_count,
    cast_count,
    for_you_page_visit_total_count,
    latino_browse_page_visit_total_count,
    kids_browse_page_visit_total_count,
    linear_browse_page_visit_total_count,
    movie_browse_page_visit_total_count,
    series_browse_page_visit_total_count,
    home_page_no_modes_visit_total_count,
    search_page_visit_total_count,
    for_you_page_registration_gate_total_count,
    complete_5p_total_count,
    complete_30p_total_count,
    complete_70p_total_count,
    complete_90p_total_count,
    movie_complete_30p_total_count,
    movie_complete_70p_total_count,
    movie_complete_90p_total_count,
    episode_complete_30p_total_count,
    episode_complete_70p_total_count,
    episode_complete_90p_total_count,
    sports_event_complete_30p_total_count,
    sports_event_complete_70p_total_count,
    sports_event_complete_90p_total_count,
    linear_view_total_count,
    -- deprecated columns to soft-delete / transition
    content_name as title,
    content_id as episode_title_id,
    '' as channel,
    program_id as title_id
from all_data_4 as x
join `hive_metastore`.`tubidw_dev`.`device_first_metrics` as d using (device_id)
left join user_data as u using (_id)
