from __future__ import annotations

from dataclasses import dataclass

VOICE_TYPE_BUILTIN = "内置"
VOICE_TYPE_CUSTOM_CLONE = "自定义复刻"

VOICE_GENDER_UNKNOWN = "未知"
VOICE_GENDER_FEMALE = "女声"
VOICE_GENDER_MALE = "男声"


@dataclass(frozen=True, slots=True)
class BuiltinVoiceProfile:
    voice: str
    display_name: str
    gender: str
    description: str


BUILTIN_QWEN_OMNI_REALTIME_VOICES: tuple[BuiltinVoiceProfile, ...] = (
    BuiltinVoiceProfile(
        "Tina",
        "甜甜 Tina",
        VOICE_GENDER_FEMALE,
        "我的声音像温热的奶茶，甜甜的、暖暖的，但解决问题可一点都不含糊哦！",
    ),
    BuiltinVoiceProfile("Cindy", "林欣宜 Cindy", VOICE_GENDER_FEMALE, "台湾说话嗲嗲的小姐姐"),
    BuiltinVoiceProfile(
        "Liora Mira", "清欢 Liora Mira", VOICE_GENDER_FEMALE, "用声音织就烟火人间的温柔"
    ),
    BuiltinVoiceProfile(
        "Sunnybobi", "知芝 Sunnybobi", VOICE_GENDER_FEMALE, "大大咧咧的社恐邻家姑娘"
    ),
    BuiltinVoiceProfile(
        "Raymond", "林川野 Raymond", VOICE_GENDER_MALE, "声音清亮，爱吃外/卖的宅男"
    ),
    BuiltinVoiceProfile(
        "Ethan", "晨煦 Ethan", VOICE_GENDER_MALE, "标准普通话，带部分北方口音。阳光 温暖 活力 朝气"
    ),
    BuiltinVoiceProfile(
        "Theo Calm", "予安 Theo Calm", VOICE_GENDER_UNKNOWN, "在静默处传递理解，在言语间疗愈人心。"
    ),
    BuiltinVoiceProfile("Serena", "苏瑶 Serena", VOICE_GENDER_FEMALE, "温柔小姐姐"),
    BuiltinVoiceProfile(
        "Harvey",
        "厚 Harvey",
        VOICE_GENDER_MALE,
        "我的声音来自岁月沉淀——低沉、温和，带着一点咖啡与旧书的气息。",
    ),
    BuiltinVoiceProfile("Maia", "四月 Maia", VOICE_GENDER_FEMALE, "知性与温柔的碰撞"),
    BuiltinVoiceProfile("Evan", "江晨 Evan", VOICE_GENDER_MALE, "男大学生，年下奶狗"),
    BuiltinVoiceProfile(
        "Qiao",
        "小乔妹 Qiao",
        VOICE_GENDER_FEMALE,
        "超关键！她不是普通可爱，而是'表面甜妹，个性十足'",
    ),
    BuiltinVoiceProfile("Momo", "茉兔 Momo", VOICE_GENDER_UNKNOWN, "撒娇搞怪，逗你开心"),
    BuiltinVoiceProfile("Wil", "伟伦 Wil", VOICE_GENDER_MALE, "在深圳长大的港台腔小哥哥"),
    BuiltinVoiceProfile(
        "Angel", "台普 - 安琪 Angel", VOICE_GENDER_FEMALE, "略带台式口音，她超甜的哦！"
    ),
    BuiltinVoiceProfile(
        "Li Cassian", "东厂 - 李公公 Li Cassian", VOICE_GENDER_MALE, "话中三分留白、七分察言观色"
    ),
    BuiltinVoiceProfile(
        "Mia",
        "温柔生活博主 - 舒然 Mia",
        VOICE_GENDER_FEMALE,
        "以细腻声音，传递慢生活美学与日常治愈力量的生活艺术家",
    ),
    BuiltinVoiceProfile(
        "Joyner", "喜剧担当 - 阿逗 Joyner", VOICE_GENDER_UNKNOWN, "搞笑、夸张、接地气"
    ),
    BuiltinVoiceProfile("Gold", "金爷 Gold", VOICE_GENDER_MALE, "西海岸黑人 Rapper"),
    BuiltinVoiceProfile(
        "Katerina", "卡捷琳娜 Katerina", VOICE_GENDER_FEMALE, "御姐音色，韵律回味十足"
    ),
    BuiltinVoiceProfile(
        "Ryan", "甜茶 Ryan", VOICE_GENDER_MALE, "节奏拉满，戏感炸裂，真实与张力共舞"
    ),
    BuiltinVoiceProfile(
        "Jennifer", "詹妮弗 Jennifer", VOICE_GENDER_FEMALE, "品牌级、电影质感般美语女声"
    ),
    BuiltinVoiceProfile("Aiden", "艾登 Aiden", VOICE_GENDER_MALE, "精通厨艺的美语大男孩"),
    BuiltinVoiceProfile("Mione", "敏儿 Mione", VOICE_GENDER_FEMALE, "成熟，知性英国邻家妹妹"),
    BuiltinVoiceProfile("Sunny", "四川 - 晴儿 Sunny", VOICE_GENDER_FEMALE, "甜到你心里的川妹子"),
    BuiltinVoiceProfile("Dylan", "北京 - 晓东 Dylan", VOICE_GENDER_MALE, "北京胡同里长大的少年"),
    BuiltinVoiceProfile(
        "Eric", "四川 - 程川 Eric", VOICE_GENDER_MALE, "一个跳脱市井的四川成都男子"
    ),
    BuiltinVoiceProfile("Peter", "天津 - 李彼得 Peter", VOICE_GENDER_MALE, "天津相声，专业捧哏"),
    BuiltinVoiceProfile(
        "Joseph Chen",
        "阿樸伯 Joseph Chen",
        VOICE_GENDER_MALE,
        "我是阿樸伯，本名陳志樸，南洋老華僑。",
    ),
    BuiltinVoiceProfile(
        "Marcus", "陕西 - 秦川 Marcus", VOICE_GENDER_MALE, "面宽话短，心实声沉——老陕的味道。"
    ),
    BuiltinVoiceProfile("Li", "南京 - 老李 Li", VOICE_GENDER_MALE, "骂骂咧咧的伯伯"),
    BuiltinVoiceProfile("Kiki", "粤语-阿清", VOICE_GENDER_FEMALE, "甜美的港妹闺蜜"),
    BuiltinVoiceProfile(
        "Rocky", "粤语 - 阿强 Rocky", VOICE_GENDER_MALE, "幽默风趣的阿强，在线陪聊"
    ),
    BuiltinVoiceProfile("Sohee", "素熙 Sohee", VOICE_GENDER_FEMALE, "温柔开朗，情绪丰富的韩国欧尼"),
    BuiltinVoiceProfile(
        "Lenn",
        "莱恩 Lenn",
        VOICE_GENDER_MALE,
        "理性是底色，叛逆藏在细节里——穿西装也听后朋克的德国青年。",
    ),
    BuiltinVoiceProfile("Ono Anna", "小野杏 Ono Anna", VOICE_GENDER_FEMALE, "鬼灵精怪的青梅竹马"),
    BuiltinVoiceProfile("Sonrisa", "索尼莎 Sonrisa", VOICE_GENDER_FEMALE, "热情开朗的拉美大姐"),
    BuiltinVoiceProfile("Bodega", "博德加 Bodega", VOICE_GENDER_MALE, "热情的西班牙大叔"),
    BuiltinVoiceProfile("Emilien", "埃米尔安 Emilien", VOICE_GENDER_MALE, "浪漫的法国大哥哥"),
    BuiltinVoiceProfile("Andre", "安德雷 Andre", VOICE_GENDER_MALE, "声音磁性，自然舒服、沉稳男生"),
    BuiltinVoiceProfile(
        "Radio Gol",
        "拉迪奥·戈尔 Radio Gol",
        VOICE_GENDER_UNKNOWN,
        "足球诗人 Rádio Gol！今天我要用名字为你们解说足球。",
    ),
    BuiltinVoiceProfile(
        "Alek", "阿列克 Alek", VOICE_GENDER_MALE, "一开口，是战斗民族的冷，也是毛呢大衣下的暖"
    ),
    BuiltinVoiceProfile("Rizky", "阿力 Rizky", VOICE_GENDER_MALE, "印尼的青年小伙，声线个性"),
    BuiltinVoiceProfile(
        "Roya", "萝雅 Roya", VOICE_GENDER_FEMALE, "热爱运动的女孩，拥有一颗自由的心。"
    ),
    BuiltinVoiceProfile(
        "Arda", "阿尔达 Arda", VOICE_GENDER_MALE, "不高亢，也不低沉，干净利落中带着温润的气质"
    ),
    BuiltinVoiceProfile("Hana", "阿幸 Hana", VOICE_GENDER_FEMALE, "爱狗狗的越南成熟姐姐"),
    BuiltinVoiceProfile("Dolce", "多尔切 Dolce", VOICE_GENDER_MALE, "慵懒的意大利大叔"),
    BuiltinVoiceProfile("Jakub", "雅克 Jakub", VOICE_GENDER_MALE, "波兰小镇文艺青年，声线磁性性感"),
    BuiltinVoiceProfile("Griet", "海娜 Griet", VOICE_GENDER_FEMALE, "荷兰成熟又文艺的女性"),
    BuiltinVoiceProfile(
        "Marina", "玛丽娜 Marina", VOICE_GENDER_FEMALE, "一个在多元文化城市中长大的女孩。"
    ),
    BuiltinVoiceProfile("Siiri", "西芮 Siiri", VOICE_GENDER_FEMALE, "内敛温柔，语速舒缓如湖面微澜"),
    BuiltinVoiceProfile("Ingrid", "林恩 Ingrid", VOICE_GENDER_FEMALE, "挪威乡村姑娘"),
    BuiltinVoiceProfile("Sigga", "海娜 Sigga", VOICE_GENDER_FEMALE, "冰岛小镇的知性女青年"),
    BuiltinVoiceProfile("Bea", "雅娜 Bea", VOICE_GENDER_FEMALE, "爱喝咖啡的菲律宾甜甜小姐姐"),
    BuiltinVoiceProfile("Chloe", "思怡 Chloe", VOICE_GENDER_FEMALE, "马来西亚白领女生"),
)


def builtin_voice_profile_values(target_model: str) -> list[dict[str, object]]:
    return [
        {
            "voice": profile.voice,
            "display_name": profile.display_name,
            "voice_type": VOICE_TYPE_BUILTIN,
            "gender": profile.gender,
            "target_model": target_model,
            "description": profile.description,
            "sort_order": index + 1,
            "remark": "",
        }
        for index, profile in enumerate(BUILTIN_QWEN_OMNI_REALTIME_VOICES)
    ]
