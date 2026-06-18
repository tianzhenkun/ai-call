create table if not exists ai_call_voice_profile (
    id bigint primary key,
    voice varchar(128) not null,
    display_name varchar(100) not null,
    voice_type varchar(32) not null,
    gender varchar(16) not null default '未知',
    target_model varchar(64) not null,
    description varchar(500) null,
    sort_order integer not null default 0,
    remark varchar(500) null default '',
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_voice_model_voice unique (target_model, voice)
);

create index if not exists idx_ai_call_voice_model_sort
    on ai_call_voice_profile (target_model, sort_order);

comment on table ai_call_voice_profile is 'AI Call 端到端音色配置表';
comment on column ai_call_voice_profile.voice is 'Qwen Realtime voice 参数';
comment on column ai_call_voice_profile.display_name is '音色展示名';
comment on column ai_call_voice_profile.voice_type is '音色类型：内置/自定义复刻';
comment on column ai_call_voice_profile.gender is '音色性别：未知/女声/男声';
comment on column ai_call_voice_profile.target_model is '适用 Qwen Omni Realtime 模型';
comment on column ai_call_voice_profile.description is '官方描述或备注说明';

insert into ai_call_voice_profile
    (id, voice, display_name, voice_type, gender, target_model, description, sort_order, remark, created_at, updated_at)
values
    (330000000000000001, 'Tina', '甜甜 Tina', '内置', '女声', 'qwen3.5-omni-plus-realtime', '我的声音像温热的奶茶，甜甜的、暖暖的，但解决问题可一点都不含糊哦！', 1, '', now(), now()),
    (330000000000000002, 'Cindy', '林欣宜 Cindy', '内置', '女声', 'qwen3.5-omni-plus-realtime', '台湾说话嗲嗲的小姐姐', 2, '', now(), now()),
    (330000000000000003, 'Liora Mira', '清欢 Liora Mira', '内置', '女声', 'qwen3.5-omni-plus-realtime', '用声音织就烟火人间的温柔', 3, '', now(), now()),
    (330000000000000004, 'Sunnybobi', '知芝 Sunnybobi', '内置', '女声', 'qwen3.5-omni-plus-realtime', '大大咧咧的社恐邻家姑娘', 4, '', now(), now()),
    (330000000000000005, 'Raymond', '林川野 Raymond', '内置', '男声', 'qwen3.5-omni-plus-realtime', '声音清亮，爱吃外/卖的宅男', 5, '', now(), now()),
    (330000000000000006, 'Ethan', '晨煦 Ethan', '内置', '男声', 'qwen3.5-omni-plus-realtime', '标准普通话，带部分北方口音。阳光 温暖 活力 朝气', 6, '', now(), now()),
    (330000000000000007, 'Theo Calm', '予安 Theo Calm', '内置', '未知', 'qwen3.5-omni-plus-realtime', '在静默处传递理解，在言语间疗愈人心。', 7, '', now(), now()),
    (330000000000000008, 'Serena', '苏瑶 Serena', '内置', '女声', 'qwen3.5-omni-plus-realtime', '温柔小姐姐', 8, '', now(), now()),
    (330000000000000009, 'Harvey', '厚 Harvey', '内置', '男声', 'qwen3.5-omni-plus-realtime', '我的声音来自岁月沉淀——低沉、温和，带着一点咖啡与旧书的气息。', 9, '', now(), now()),
    (330000000000000010, 'Maia', '四月 Maia', '内置', '女声', 'qwen3.5-omni-plus-realtime', '知性与温柔的碰撞', 10, '', now(), now()),
    (330000000000000011, 'Evan', '江晨 Evan', '内置', '男声', 'qwen3.5-omni-plus-realtime', '男大学生，年下奶狗', 11, '', now(), now()),
    (330000000000000012, 'Qiao', '小乔妹 Qiao', '内置', '女声', 'qwen3.5-omni-plus-realtime', '超关键！她不是普通可爱，而是''表面甜妹，个性十足''', 12, '', now(), now()),
    (330000000000000013, 'Momo', '茉兔 Momo', '内置', '未知', 'qwen3.5-omni-plus-realtime', '撒娇搞怪，逗你开心', 13, '', now(), now()),
    (330000000000000014, 'Wil', '伟伦 Wil', '内置', '男声', 'qwen3.5-omni-plus-realtime', '在深圳长大的港台腔小哥哥', 14, '', now(), now()),
    (330000000000000015, 'Angel', '台普 - 安琪 Angel', '内置', '女声', 'qwen3.5-omni-plus-realtime', '略带台式口音，她超甜的哦！', 15, '', now(), now()),
    (330000000000000016, 'Li Cassian', '东厂 - 李公公 Li Cassian', '内置', '男声', 'qwen3.5-omni-plus-realtime', '话中三分留白、七分察言观色', 16, '', now(), now()),
    (330000000000000017, 'Mia', '温柔生活博主 - 舒然 Mia', '内置', '女声', 'qwen3.5-omni-plus-realtime', '以细腻声音，传递慢生活美学与日常治愈力量的生活艺术家', 17, '', now(), now()),
    (330000000000000018, 'Joyner', '喜剧担当 - 阿逗 Joyner', '内置', '未知', 'qwen3.5-omni-plus-realtime', '搞笑、夸张、接地气', 18, '', now(), now()),
    (330000000000000019, 'Gold', '金爷 Gold', '内置', '男声', 'qwen3.5-omni-plus-realtime', '西海岸黑人 Rapper', 19, '', now(), now()),
    (330000000000000020, 'Katerina', '卡捷琳娜 Katerina', '内置', '女声', 'qwen3.5-omni-plus-realtime', '御姐音色，韵律回味十足', 20, '', now(), now()),
    (330000000000000021, 'Ryan', '甜茶 Ryan', '内置', '男声', 'qwen3.5-omni-plus-realtime', '节奏拉满，戏感炸裂，真实与张力共舞', 21, '', now(), now()),
    (330000000000000022, 'Jennifer', '詹妮弗 Jennifer', '内置', '女声', 'qwen3.5-omni-plus-realtime', '品牌级、电影质感般美语女声', 22, '', now(), now()),
    (330000000000000023, 'Aiden', '艾登 Aiden', '内置', '男声', 'qwen3.5-omni-plus-realtime', '精通厨艺的美语大男孩', 23, '', now(), now()),
    (330000000000000024, 'Mione', '敏儿 Mione', '内置', '女声', 'qwen3.5-omni-plus-realtime', '成熟，知性英国邻家妹妹', 24, '', now(), now()),
    (330000000000000025, 'Sunny', '四川 - 晴儿 Sunny', '内置', '女声', 'qwen3.5-omni-plus-realtime', '甜到你心里的川妹子', 25, '', now(), now()),
    (330000000000000026, 'Dylan', '北京 - 晓东 Dylan', '内置', '男声', 'qwen3.5-omni-plus-realtime', '北京胡同里长大的少年', 26, '', now(), now()),
    (330000000000000027, 'Eric', '四川 - 程川 Eric', '内置', '男声', 'qwen3.5-omni-plus-realtime', '一个跳脱市井的四川成都男子', 27, '', now(), now()),
    (330000000000000028, 'Peter', '天津 - 李彼得 Peter', '内置', '男声', 'qwen3.5-omni-plus-realtime', '天津相声，专业捧哏', 28, '', now(), now()),
    (330000000000000029, 'Joseph Chen', '阿樸伯 Joseph Chen', '内置', '男声', 'qwen3.5-omni-plus-realtime', '我是阿樸伯，本名陳志樸，南洋老華僑。', 29, '', now(), now()),
    (330000000000000030, 'Marcus', '陕西 - 秦川 Marcus', '内置', '男声', 'qwen3.5-omni-plus-realtime', '面宽话短，心实声沉——老陕的味道。', 30, '', now(), now()),
    (330000000000000031, 'Li', '南京 - 老李 Li', '内置', '男声', 'qwen3.5-omni-plus-realtime', '骂骂咧咧的伯伯', 31, '', now(), now()),
    (330000000000000032, 'Kiki', '粤语-阿清', '内置', '女声', 'qwen3.5-omni-plus-realtime', '甜美的港妹闺蜜', 32, '', now(), now()),
    (330000000000000033, 'Rocky', '粤语 - 阿强 Rocky', '内置', '男声', 'qwen3.5-omni-plus-realtime', '幽默风趣的阿强，在线陪聊', 33, '', now(), now()),
    (330000000000000034, 'Sohee', '素熙 Sohee', '内置', '女声', 'qwen3.5-omni-plus-realtime', '温柔开朗，情绪丰富的韩国欧尼', 34, '', now(), now()),
    (330000000000000035, 'Lenn', '莱恩 Lenn', '内置', '男声', 'qwen3.5-omni-plus-realtime', '理性是底色，叛逆藏在细节里——穿西装也听后朋克的德国青年。', 35, '', now(), now()),
    (330000000000000036, 'Ono Anna', '小野杏 Ono Anna', '内置', '女声', 'qwen3.5-omni-plus-realtime', '鬼灵精怪的青梅竹马', 36, '', now(), now()),
    (330000000000000037, 'Sonrisa', '索尼莎 Sonrisa', '内置', '女声', 'qwen3.5-omni-plus-realtime', '热情开朗的拉美大姐', 37, '', now(), now()),
    (330000000000000038, 'Bodega', '博德加 Bodega', '内置', '男声', 'qwen3.5-omni-plus-realtime', '热情的西班牙大叔', 38, '', now(), now()),
    (330000000000000039, 'Emilien', '埃米尔安 Emilien', '内置', '男声', 'qwen3.5-omni-plus-realtime', '浪漫的法国大哥哥', 39, '', now(), now()),
    (330000000000000040, 'Andre', '安德雷 Andre', '内置', '男声', 'qwen3.5-omni-plus-realtime', '声音磁性，自然舒服、沉稳男生', 40, '', now(), now()),
    (330000000000000041, 'Radio Gol', '拉迪奥·戈尔 Radio Gol', '内置', '未知', 'qwen3.5-omni-plus-realtime', '足球诗人 Rádio Gol！今天我要用名字为你们解说足球。', 41, '', now(), now()),
    (330000000000000042, 'Alek', '阿列克 Alek', '内置', '男声', 'qwen3.5-omni-plus-realtime', '一开口，是战斗民族的冷，也是毛呢大衣下的暖', 42, '', now(), now()),
    (330000000000000043, 'Rizky', '阿力 Rizky', '内置', '男声', 'qwen3.5-omni-plus-realtime', '印尼的青年小伙，声线个性', 43, '', now(), now()),
    (330000000000000044, 'Roya', '萝雅 Roya', '内置', '女声', 'qwen3.5-omni-plus-realtime', '热爱运动的女孩，拥有一颗自由的心。', 44, '', now(), now()),
    (330000000000000045, 'Arda', '阿尔达 Arda', '内置', '男声', 'qwen3.5-omni-plus-realtime', '不高亢，也不低沉，干净利落中带着温润的气质', 45, '', now(), now()),
    (330000000000000046, 'Hana', '阿幸 Hana', '内置', '女声', 'qwen3.5-omni-plus-realtime', '爱狗狗的越南成熟姐姐', 46, '', now(), now()),
    (330000000000000047, 'Dolce', '多尔切 Dolce', '内置', '男声', 'qwen3.5-omni-plus-realtime', '慵懒的意大利大叔', 47, '', now(), now()),
    (330000000000000048, 'Jakub', '雅克 Jakub', '内置', '男声', 'qwen3.5-omni-plus-realtime', '波兰小镇文艺青年，声线磁性性感', 48, '', now(), now()),
    (330000000000000049, 'Griet', '海娜 Griet', '内置', '女声', 'qwen3.5-omni-plus-realtime', '荷兰成熟又文艺的女性', 49, '', now(), now()),
    (330000000000000050, 'Marina', '玛丽娜 Marina', '内置', '女声', 'qwen3.5-omni-plus-realtime', '一个在多元文化城市中长大的女孩。', 50, '', now(), now()),
    (330000000000000051, 'Siiri', '西芮 Siiri', '内置', '女声', 'qwen3.5-omni-plus-realtime', '内敛温柔，语速舒缓如湖面微澜', 51, '', now(), now()),
    (330000000000000052, 'Ingrid', '林恩 Ingrid', '内置', '女声', 'qwen3.5-omni-plus-realtime', '挪威乡村姑娘', 52, '', now(), now()),
    (330000000000000053, 'Sigga', '海娜 Sigga', '内置', '女声', 'qwen3.5-omni-plus-realtime', '冰岛小镇的知性女青年', 53, '', now(), now()),
    (330000000000000054, 'Bea', '雅娜 Bea', '内置', '女声', 'qwen3.5-omni-plus-realtime', '爱喝咖啡的菲律宾甜甜小姐姐', 54, '', now(), now()),
    (330000000000000055, 'Chloe', '思怡 Chloe', '内置', '女声', 'qwen3.5-omni-plus-realtime', '马来西亚白领女生', 55, '', now(), now())
on conflict (target_model, voice) do nothing;
